//@ts-check
/// <reference lib="esnext" />
/// <reference types="@cloudflare/workers-types" />
import { DurableObject } from "cloudflare:workers";

/**
 * @typedef Env
 * @property {DurableObjectNamespace<MainDO>} MAIN_DO
 * @property {DurableObjectNamespace<TaskDO>} TASK_DO
 */

export default {
  /** @param {Request} request @param {Env} env @param {ExecutionContext} ctx @returns {Promise<Response>} */
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    // API routes
    const mainDO = env.MAIN_DO.get(env.MAIN_DO.idFromName("main"));

    if (path === "/api/tasks" && request.method === "POST") {
      return mainDO.createTask(request, env);
    } else if (path === "/api/tasks" && request.method === "GET") {
      return mainDO.getTasks();
    } else if (path.startsWith("/api/task/") && request.method === "GET") {
      const taskId = path.split("/")[3];
      return mainDO.getTask(taskId);
    }

    return new Response("Not found", { status: 404 });
  },
};

export class MainDO extends DurableObject {
  /** @param {DurableObjectState} state @param {Env} env */
  constructor(state, env) {
    super(state, env);
    this.sql = state.storage.sql;
    this.env = env;

    // Initialize tables
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        input TEXT NOT NULL,
        processor TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        run_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS task_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_data TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        FOREIGN KEY (task_id) REFERENCES tasks (id)
      )
    `);
  }

  /** @param {Request} request @param {Env} env */
  async createTask(request, env) {
    const { input, processor, apiKey } = await request.json();

    if (!input || !processor || !apiKey) {
      return new Response(
        JSON.stringify({ error: "Missing required fields" }),
        {
          status: 400,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const taskId = crypto.randomUUID();
    const now = new Date().toISOString();

    // Store task in database
    this.sql.exec(
      "INSERT INTO tasks (id, input, processor, status, created_at, updated_at) VALUES (?, ?, ?, 'queued', ?, ?)",
      taskId,
      input,
      processor,
      now,
      now
    );

    // Create TaskDO and start processing
    const taskDO = env.TASK_DO.get(env.TASK_DO.idFromName(taskId));
    taskDO.startTask(taskId, input, processor, apiKey);

    return new Response(JSON.stringify({ taskId }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  async getTasks() {
    const tasks = this.sql
      .exec("SELECT * FROM tasks ORDER BY created_at DESC")
      .toArray();
    return new Response(JSON.stringify(tasks), {
      headers: { "Content-Type": "application/json" },
    });
  }

  /** @param {string} taskId */
  async getTask(taskId) {
    const task = this.sql
      .exec("SELECT * FROM tasks WHERE id = ?", taskId)
      .toArray()[0];
    if (!task) {
      return new Response(JSON.stringify({ error: "Task not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      });
    }

    const events = this.sql
      .exec(
        "SELECT * FROM task_events WHERE task_id = ? ORDER BY timestamp ASC",
        taskId
      )
      .toArray();

    return new Response(JSON.stringify({ task, events }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  /** @param {string} taskId @param {string} status @param {string} runId */
  async updateTaskStatus(taskId, status, runId = null) {
    const now = new Date().toISOString();
    if (runId) {
      this.sql.exec(
        "UPDATE tasks SET status = ?, run_id = ?, updated_at = ? WHERE id = ?",
        status,
        runId,
        now,
        taskId
      );
    } else {
      this.sql.exec(
        "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
        status,
        now,
        taskId
      );
    }
  }

  /** @param {string} taskId @param {string} eventType @param {any} eventData */
  async addTaskEvent(taskId, eventType, eventData) {
    const now = new Date().toISOString();
    this.sql.exec(
      "INSERT INTO task_events (task_id, event_type, event_data, timestamp) VALUES (?, ?, ?, ?)",
      taskId,
      eventType,
      JSON.stringify(eventData),
      now
    );
  }
}

export class TaskDO extends DurableObject {
  /** @param {DurableObjectState} state @param {Env} env */
  constructor(state, env) {
    super(state, env);
    this.env = env;
  }

  /** @param {string} taskId @param {string} input @param {string} processor @param {string} apiKey */
  async startTask(taskId, input, processor, apiKey) {
    const mainDO = this.env.MAIN_DO.get(this.env.MAIN_DO.idFromName("main"));

    try {
      // Create task run via Parallel API
      const createResponse = await fetch(
        "https://api.parallel.ai/v1/tasks/runs",
        {
          method: "POST",
          headers: {
            "x-api-key": apiKey,
            "content-type": "application/json",
            "parallel-beta": "events-sse-2025-07-24",
          },
          body: JSON.stringify({
            input,
            processor,
            enable_events: true,
          }),
        }
      );

      if (!createResponse.ok) {
        const error = await createResponse.text();
        await mainDO.updateTaskStatus(taskId, "failed");
        await mainDO.addTaskEvent(taskId, "error", { message: error });
        return;
      }

      const taskRun = await createResponse.json();
      const runId = taskRun.run_id;

      await mainDO.updateTaskStatus(taskId, "running", runId);
      await mainDO.addTaskEvent(taskId, "created", taskRun);

      // Start SSE stream for events
      this.streamEvents(taskId, runId, apiKey, mainDO);

      // Poll for completion
      this.pollForCompletion(taskId, runId, apiKey, mainDO);
    } catch (error) {
      await mainDO.updateTaskStatus(taskId, "failed");
      await mainDO.addTaskEvent(taskId, "error", { message: error.message });
    }
  }

  /** @param {string} taskId @param {string} runId @param {string} apiKey @param {any} mainDO */
  async streamEvents(taskId, runId, apiKey, mainDO) {
    try {
      const eventResponse = await fetch(
        `https://api.parallel.ai/v1beta/tasks/runs/${runId}/events`,
        {
          headers: {
            "x-api-key": apiKey,
            "content-type": "text/event-stream",
          },
        }
      );

      if (!eventResponse.ok) return;

      const reader = eventResponse.body?.getReader();
      if (!reader) return;

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const eventData = JSON.parse(line.slice(6));
              await mainDO.addTaskEvent(
                taskId,
                eventData.type || "stream",
                eventData
              );
            } catch (e) {
              // Ignore malformed JSON
            }
          }
        }
      }
    } catch (error) {
      await mainDO.addTaskEvent(taskId, "stream_error", {
        message: error.message,
      });
    }
  }

  /** @param {string} taskId @param {string} runId @param {string} apiKey @param {any} mainDO */
  async pollForCompletion(taskId, runId, apiKey, mainDO) {
    const maxAttempts = 60; // 10 minutes max
    let attempts = 0;

    while (attempts < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 10000)); // Wait 10 seconds
      attempts++;

      try {
        const statusResponse = await fetch(
          `https://api.parallel.ai/v1/tasks/runs/${runId}`,
          {
            headers: { "x-api-key": apiKey },
          }
        );

        if (!statusResponse.ok) continue;

        const taskRun = await statusResponse.json();

        if (taskRun.status === "completed") {
          // Get results
          const resultResponse = await fetch(
            `https://api.parallel.ai/v1/tasks/runs/${runId}/result`,
            {
              headers: { "x-api-key": apiKey },
            }
          );

          if (resultResponse.ok) {
            const result = await resultResponse.json();
            await mainDO.updateTaskStatus(taskId, "completed");
            await mainDO.addTaskEvent(taskId, "completed", result);
          }
          break;
        } else if (taskRun.status === "failed") {
          await mainDO.updateTaskStatus(taskId, "failed");
          await mainDO.addTaskEvent(taskId, "failed", taskRun);
          break;
        }
      } catch (error) {
        await mainDO.addTaskEvent(taskId, "poll_error", {
          message: error.message,
        });
      }
    }
  }
}
