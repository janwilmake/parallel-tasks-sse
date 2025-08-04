//@ts-check
/// <reference lib="esnext" />
/// <reference types="@cloudflare/workers-types" />
import { DurableObject } from "cloudflare:workers";

/**
 * @typedef Env
 * @property {DurableObjectNamespace<TaskManager>} TASK_MANAGER
 * @property {DurableObjectNamespace<TaskRunner>} TASK_RUNNER
 */

export default {
  /** @param {Request} request @param {Env} env @param {ExecutionContext} ctx @returns {Promise<Response>} */
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Get the main task manager DO
    const taskManagerId = env.TASK_MANAGER.idFromName("main");
    const taskManager = env.TASK_MANAGER.get(taskManagerId);

    if (url.pathname === "/api/tasks" && request.method === "POST") {
      return taskManager.createTask(await request.json());
    }

    if (url.pathname === "/api/tasks" && request.method === "GET") {
      return taskManager.getTasks();
    }

    if (url.pathname.startsWith("/task/")) {
      const taskId = url.pathname.split("/")[2];
      return taskManager.getTaskDetails(taskId);
    }

    return new Response("Not found", { status: 404 });
  },
};

export class TaskManager extends DurableObject {
  /** @param {DurableObjectState} state @param {Env} env */
  constructor(state, env) {
    super(state, env);
    this.sql = state.storage.sql;
    this.env = env;

    // Initialize tables
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        api_key TEXT NOT NULL,
        processor TEXT NOT NULL,
        input TEXT NOT NULL,
        task_spec TEXT,
        run_id TEXT,
        status TEXT DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        completed_at INTEGER,
        result TEXT
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS task_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_data TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        FOREIGN KEY (task_id) REFERENCES tasks (id)
      )
    `);
  }

  /** @param {any} taskData  */
  async createTask(taskData) {
    const taskId = crypto.randomUUID();
    const now = Date.now();

    // Store task in database
    this.sql.exec(
      `INSERT INTO tasks (id, api_key, processor, input, task_spec, created_at) 
       VALUES (?, ?, ?, ?, ?, ?)`,
      taskId,
      taskData.apiKey,
      taskData.processor,
      taskData.input,
      taskData.taskSpec ? JSON.stringify(taskData.taskSpec) : null,
      now
    );

    // Create a task runner DO for this specific task
    const taskRunnerId = this.env.TASK_RUNNER.idFromName(taskId);
    const taskRunner = this.env.TASK_RUNNER.get(taskRunnerId);

    // Start the task runner (fire and forget)
    taskRunner.runTask(taskId, taskData);

    return new Response(JSON.stringify({ taskId, status: "started" }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  async getTasks() {
    const result = this.sql.exec(`
      SELECT id, processor, status, created_at, completed_at, 
             substr(input, 1, 100) as input_preview
      FROM tasks 
      ORDER BY created_at DESC
    `);

    const tasks = result.toArray().map((row) => ({
      id: row.id,
      processor: row.processor,
      status: row.status,
      createdAt: new Date(row.created_at).toISOString(),
      completedAt: row.completed_at
        ? new Date(row.completed_at).toISOString()
        : null,
      inputPreview: row.input_preview,
    }));

    return new Response(JSON.stringify(tasks), {
      headers: { "Content-Type": "application/json" },
    });
  }

  /** @param {string} taskId */
  async getTaskDetails(taskId) {
    // Get task info
    const taskResult = this.sql.exec(
      `SELECT * FROM tasks WHERE id = ?`,
      taskId
    );
    const taskRows = taskResult.toArray();

    if (taskRows.length === 0) {
      return new Response("Task not found", { status: 404 });
    }

    const task = taskRows[0];

    // Get all events for this task
    const eventsResult = this.sql.exec(
      `
      SELECT event_type, event_data, timestamp 
      FROM task_events 
      WHERE task_id = ? 
      ORDER BY timestamp ASC
    `,
      taskId
    );

    const events = eventsResult.toArray().map((row) => ({
      type: row.event_type,
      data: JSON.parse(row.event_data),
      timestamp: new Date(row.timestamp).toISOString(),
    }));

    const response = {
      task: {
        id: task.id,
        processor: task.processor,
        input: task.input,
        taskSpec: task.task_spec ? JSON.parse(task.task_spec) : null,
        runId: task.run_id,
        status: task.status,
        createdAt: new Date(task.created_at).toISOString(),
        completedAt: task.completed_at
          ? new Date(task.completed_at).toISOString()
          : null,
        result: task.result ? JSON.parse(task.result) : null,
      },
      events,
    };

    return new Response(JSON.stringify(response, null, 2), {
      headers: { "Content-Type": "application/json" },
    });
  }

  /** @param {string} taskId @param {string} eventType @param {any} eventData */
  async addEvent(taskId, eventType, eventData) {
    this.sql.exec(
      `INSERT INTO task_events (task_id, event_type, event_data, timestamp) 
       VALUES (?, ?, ?, ?)`,
      taskId,
      eventType,
      JSON.stringify(eventData),
      Date.now()
    );
  }

  /** @param {string} taskId @param {string} runId */
  async updateTaskRunId(taskId, runId) {
    this.sql.exec(`UPDATE tasks SET run_id = ? WHERE id = ?`, runId, taskId);
  }

  /** @param {string} taskId @param {string} status @param {any} result */
  async updateTaskStatus(taskId, status, result = null) {
    const completedAt =
      status === "completed" || status === "failed" ? Date.now() : null;
    this.sql.exec(
      `UPDATE tasks SET status = ?, completed_at = ?, result = ? WHERE id = ?`,
      status,
      completedAt,
      result ? JSON.stringify(result) : null,
      taskId
    );
  }
}

export class TaskRunner extends DurableObject {
  /** @param {DurableObjectState} state @param {Env} env */
  constructor(state, env) {
    super(state, env);
    this.env = env;
  }

  /** @param {string} taskId @param {any} taskData */
  async runTask(taskId, taskData) {
    try {
      // Get the main task manager to report back to
      const taskManagerId = this.env.TASK_MANAGER.idFromName("main");
      const taskManager = this.env.TASK_MANAGER.get(taskManagerId);

      // Create the task run
      const createPayload = {
        input: taskData.input,
        processor: taskData.processor,
        enable_events: true,
      };

      if (taskData.taskSpec) {
        createPayload.task_spec = taskData.taskSpec;
      }

      const createResponse = await fetch(
        "https://api.parallel.ai/v1/tasks/runs",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-api-key": taskData.apiKey,
            "parallel-beta": "events-sse-2025-07-24",
          },
          body: JSON.stringify(createPayload),
        }
      );

      if (!createResponse.ok) {
        const error = await createResponse.text();
        await taskManager.addEvent(taskId, "error", {
          message: `Failed to create task: ${error}`,
        });
        await taskManager.updateTaskStatus(taskId, "failed");
        return;
      }

      const taskRun = await createResponse.json();
      const runId = taskRun.run_id;

      await taskManager.updateTaskRunId(taskId, runId);
      await taskManager.addEvent(taskId, "task_created", taskRun);
      await taskManager.updateTaskStatus(taskId, "running");

      // Start listening to SSE events
      const eventsResponse = await fetch(
        `https://api.parallel.ai/v1beta/tasks/runs/${runId}/events`,
        {
          headers: {
            "x-api-key": taskData.apiKey,
            "Content-Type": "text/event-stream",
          },
        }
      );

      if (!eventsResponse.ok) {
        await taskManager.addEvent(taskId, "error", {
          message: "Failed to connect to SSE stream",
        });
        await taskManager.updateTaskStatus(taskId, "failed");
        return;
      }

      // Process SSE stream
      const reader = eventsResponse.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      if (!reader) {
        await taskManager.addEvent(taskId, "error", {
          message: "No readable stream",
        });
        await taskManager.updateTaskStatus(taskId, "failed");
        return;
      }

      try {
        while (true) {
          const { done, value } = await reader.read();

          if (done) {
            // Stream ended, check final status
            await this.checkFinalStatus(
              taskId,
              runId,
              taskData.apiKey,
              taskManager
            );
            break;
          }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.startsWith("data: ")) {
              try {
                const eventData = JSON.parse(line.slice(6));
                await taskManager.addEvent(taskId, "sse_event", eventData);

                // Check if it's a status event indicating completion
                if (eventData.type === "status") {
                  if (eventData.status === "completed") {
                    // Get the final result
                    await this.fetchAndStoreResult(
                      taskId,
                      runId,
                      taskData.apiKey,
                      taskManager
                    );
                    return; // Exit the function
                  } else if (eventData.status === "failed") {
                    await taskManager.updateTaskStatus(taskId, "failed");
                    return; // Exit the function
                  }
                }
              } catch (e) {
                await taskManager.addEvent(taskId, "parse_error", {
                  message: `Failed to parse SSE event: ${e.message}`,
                  line: line,
                });
              }
            }
          }
        }
      } finally {
        reader.releaseLock();
      }
    } catch (error) {
      const taskManagerId = this.env.TASK_MANAGER.idFromName("main");
      const taskManager = this.env.TASK_MANAGER.get(taskManagerId);
      await taskManager.addEvent(taskId, "error", { message: error.message });
      await taskManager.updateTaskStatus(taskId, "failed");
    }
  }

  /** @param {string} taskId @param {string} runId @param {string} apiKey @param {any} taskManager */
  async fetchAndStoreResult(taskId, runId, apiKey, taskManager) {
    try {
      const resultResponse = await fetch(
        `https://api.parallel.ai/v1/tasks/runs/${runId}/result`,
        {
          headers: {
            "x-api-key": apiKey,
          },
        }
      );

      if (resultResponse.ok) {
        const result = await resultResponse.json();
        await taskManager.addEvent(taskId, "result", result);
        await taskManager.updateTaskStatus(taskId, "completed", result);
      } else {
        const errorText = await resultResponse.text();
        await taskManager.addEvent(taskId, "result_error", {
          message: `Failed to fetch result: ${errorText}`,
          status: resultResponse.status,
        });
        await taskManager.updateTaskStatus(taskId, "failed");
      }
    } catch (error) {
      await taskManager.addEvent(taskId, "result_error", {
        message: `Error fetching result: ${error.message}`,
      });
      await taskManager.updateTaskStatus(taskId, "failed");
    }
  }

  /** @param {string} taskId @param {string} runId @param {string} apiKey @param {any} taskManager */
  async checkFinalStatus(taskId, runId, apiKey, taskManager) {
    try {
      // Stream ended without completion status, check current status
      const statusResponse = await fetch(
        `https://api.parallel.ai/v1/tasks/runs/${runId}`,
        {
          headers: {
            "x-api-key": apiKey,
          },
        }
      );

      if (statusResponse.ok) {
        const status = await statusResponse.json();
        await taskManager.addEvent(taskId, "final_status_check", status);

        if (status.status === "completed") {
          await this.fetchAndStoreResult(taskId, runId, apiKey, taskManager);
        } else if (status.status === "failed") {
          await taskManager.updateTaskStatus(taskId, "failed");
        } else {
          // Still running or other status
          await taskManager.updateTaskStatus(taskId, status.status);
        }
      } else {
        await taskManager.addEvent(taskId, "status_check_error", {
          message: "Failed to check final status",
        });
        await taskManager.updateTaskStatus(taskId, "unknown");
      }
    } catch (error) {
      await taskManager.addEvent(taskId, "status_check_error", {
        message: `Error checking final status: ${error.message}`,
      });
      await taskManager.updateTaskStatus(taskId, "unknown");
    }
  }
}
