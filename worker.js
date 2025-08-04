//@ts-check
/// <reference lib="esnext" />
/// <reference types="@cloudflare/workers-types" />
import { DurableObject } from "cloudflare:workers";
//@ts-ignore
import indexHtml from "./public/index.html";

/**
 * @typedef Env
 * @property {DurableObjectNamespace<TaskStoreDO>} TASKSTORE
 * @property {string} PARALLEL_API_KEY - Parallel API key
 */

export default {
  /** @param {Request} request @param {Env} env @param {ExecutionContext} ctx @returns {Promise<Response>} */
  async fetch(request, env, ctx) {
    // Check for required environment variables
    if (!env.PARALLEL_API_KEY) {
      return new Response("Missing PARALLEL_API_KEY environment variable", {
        status: 500,
      });
    }

    const url = new URL(request.url);
    const taskStore = env.TASKSTORE.get(env.TASKSTORE.idFromName("main"));

    if (url.pathname === "/" && request.method === "GET") {
      return new Response(indexHtml, {
        headers: { "Content-Type": "text/html" },
      });
    }

    if (url.pathname === "/api/tasks" && request.method === "POST") {
      const { input, processor = "lite" } = await request.json();
      if (!input) {
        return new Response(JSON.stringify({ error: "Input is required" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      const result = await taskStore.createTask(
        input,
        processor,
        env.PARALLEL_API_KEY
      );
      return new Response(JSON.stringify(result), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (url.pathname === "/api/tasks" && request.method === "GET") {
      const tasks = await taskStore.getAllTasks();
      return new Response(JSON.stringify(tasks), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (url.pathname.startsWith("/task/") && request.method === "GET") {
      const taskId = url.pathname.split("/")[2];
      const task = await taskStore.getTask(taskId);
      if (!task) {
        return new Response("Task not found", { status: 404 });
      }

      return new Response(JSON.stringify(task, null, 2), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (
      url.pathname.startsWith("/api/task/") &&
      url.pathname.endsWith("/detail") &&
      request.method === "GET"
    ) {
      const taskId = url.pathname.split("/")[3];
      const task = await taskStore.getTask(taskId);
      return new Response(JSON.stringify(task || null), {
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response("Not Found", { status: 404 });
  },
};

export class TaskStoreDO extends DurableObject {
  /** @param {string} name */
  get = (name) => this.env.TASKSTORE.get(this.env.TASKSTORE.idFromName(name));

  /** @param {DurableObjectState} state @param {Env} env */
  constructor(state, env) {
    super(state, env);
    this.sql = state.storage.sql;
    this.env = env;
    this.initTables();
  }

  initTables() {
    // Create tasks table
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        input TEXT NOT NULL,
        processor TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        result TEXT,
        parallel_run_id TEXT
      )
    `);

    // Create events table
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

  /**
   * @param {string} input
   * @param {string} processor
   * @param {string} apiKey
   */
  async createTask(input, processor, apiKey) {
    const taskId = crypto.randomUUID();
    const now = new Date().toISOString();

    // Insert task into database
    this.sql.exec(
      "INSERT INTO tasks (id, input, processor, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
      taskId,
      input,
      processor,
      "creating",
      now,
      now
    );

    // Create task in Parallel API
    try {
      const createResponse = await fetch(
        "https://api.parallel.ai/v1/tasks/runs",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-api-key": apiKey,
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
        throw new Error(
          `Failed to create task: ${createResponse.status} ${createResponse.statusText}`
        );
      }

      const taskRun = await createResponse.json();

      // Update task with parallel run ID
      this.sql.exec(
        "UPDATE tasks SET parallel_run_id = ?, status = ?, updated_at = ? WHERE id = ?",
        taskRun.run_id,
        taskRun.status,
        new Date().toISOString(),
        taskId
      );

      // Start listening to SSE events
      this.listenToEvents(taskId, taskRun.run_id, apiKey);

      return { taskId, parallelRunId: taskRun.run_id, status: taskRun.status };
    } catch (error) {
      // Update task status to failed
      this.sql.exec(
        "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
        "failed",
        new Date().toISOString(),
        taskId
      );

      // Store error event
      this.sql.exec(
        "INSERT INTO task_events (task_id, event_type, event_data, timestamp) VALUES (?, ?, ?, ?)",
        taskId,
        "error",
        JSON.stringify({ error: error.message }),
        new Date().toISOString()
      );

      throw error;
    }
  }

  /**
   * @param {string} taskId
   * @param {string} parallelRunId
   * @param {string} apiKey
   */
  async listenToEvents(taskId, parallelRunId, apiKey) {
    try {
      const sseResponse = await fetch(
        `https://api.parallel.ai/v1beta/tasks/runs/${parallelRunId}/events`,
        {
          headers: {
            "x-api-key": apiKey,
            Accept: "text/event-stream",
          },
        }
      );

      if (!sseResponse.ok) {
        throw new Error(`Failed to connect to SSE: ${sseResponse.status}`);
      }

      const reader = sseResponse.body?.getReader();
      if (!reader) {
        throw new Error("No reader available for SSE stream");
      }

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
              await this.handleEvent(taskId, eventData);
            } catch (e) {
              console.error("Failed to parse SSE event:", e);
            }
          }
        }
      }

      // Get final result
      await this.getFinalResult(taskId, parallelRunId, apiKey);
    } catch (error) {
      console.error("SSE Error:", error);
      this.sql.exec(
        "INSERT INTO task_events (task_id, event_type, event_data, timestamp) VALUES (?, ?, ?, ?)",
        taskId,
        "sse_error",
        JSON.stringify({ error: error.message }),
        new Date().toISOString()
      );
    }
  }

  /**
   * @param {string} taskId
   * @param {any} eventData
   */
  async handleEvent(taskId, eventData) {
    const now = new Date().toISOString();

    // Store event
    this.sql.exec(
      "INSERT INTO task_events (task_id, event_type, event_data, timestamp) VALUES (?, ?, ?, ?)",
      taskId,
      eventData.type || "unknown",
      JSON.stringify(eventData),
      now
    );

    // Update task status if it's a status event
    if (eventData.type === "run_status" && eventData.status) {
      this.sql.exec(
        "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
        eventData.status,
        now,
        taskId
      );
    }
  }

  /**
   * @param {string} taskId
   * @param {string} parallelRunId
   * @param {string} apiKey
   */
  async getFinalResult(taskId, parallelRunId, apiKey) {
    try {
      const resultResponse = await fetch(
        `https://api.parallel.ai/v1/tasks/runs/${parallelRunId}/result`,
        {
          headers: {
            "x-api-key": apiKey,
          },
        }
      );

      if (resultResponse.ok) {
        const result = await resultResponse.json();
        const now = new Date().toISOString();

        // Store final result
        this.sql.exec(
          "UPDATE tasks SET result = ?, status = ?, updated_at = ? WHERE id = ?",
          JSON.stringify(result),
          "completed",
          now,
          taskId
        );

        // Store result event
        this.sql.exec(
          "INSERT INTO task_events (task_id, event_type, event_data, timestamp) VALUES (?, ?, ?, ?)",
          taskId,
          "final_result",
          JSON.stringify(result),
          now
        );
      }
    } catch (error) {
      console.error("Failed to get final result:", error);
    }
  }

  async getAllTasks() {
    const result = this.sql.exec(
      "SELECT * FROM tasks ORDER BY created_at DESC"
    );
    return result.toArray();
  }

  /**
   * @param {string} taskId
   */
  async getTask(taskId) {
    const taskResult = this.sql.exec(
      "SELECT * FROM tasks WHERE id = ?",
      taskId
    );
    const tasks = taskResult.toArray();

    if (tasks.length === 0) {
      return null;
    }

    const task = tasks[0];

    // Get all events for this task
    const eventsResult = this.sql.exec(
      "SELECT * FROM task_events WHERE task_id = ? ORDER BY timestamp ASC",
      taskId
    );
    const events = eventsResult.toArray().map((event) => ({
      ...event,
      event_data: JSON.parse(event.event_data),
    }));

    return {
      ...task,
      result: task.result ? JSON.parse(task.result) : null,
      events,
    };
  }
}
