# Parallel Task Manager with SSE Streaming

A comprehensive task management system for the Parallel Task API with real-time Server-Sent Events (SSE) streaming support. This application demonstrates all three output schema modes and provides live progress monitoring.

## Features

### 1. Three Output Schema Modes

**Text Mode**

- Simple narrative responses for explanatory answers and summaries
- Ideal for questions requiring descriptive content rather than structured data
- Example: "Provide a brief overview of Tesla's business model"

**Auto Mode**

- System automatically generates optimal JSON structure based on your research query
- Best for comprehensive research reports and exploratory analysis
- Provides detailed field-level citations and verification
- Example: "Create a market research report on the EV industry"

**Object Mode**

- You define exact JSON schema with specific fields, types, and validation
- Perfect for systematic data processing and consistent outputs across multiple queries
- Example: CRM enrichment, due diligence workflows

### 2. SSE Streaming Mode

- **Real-time Events**: Captures and displays live progress updates during task execution
- **Event Storage**: All SSE events are stored in SQLite for complete audit trails
- **Status Monitoring**: Watch tasks progress from creation to completion
- **Auto-refresh**: Task list updates automatically every 10 seconds

### 3. Output Basis & Verification

The system provides comprehensive verification through the "basis" object:

**Text Mode Basis**: Basic citations and reasoning for the text response

**Auto Mode Basis**:

- Granular field-level citations using nested notation (e.g., `company_profiles.0.revenue`)
- Detailed excerpts from source materials
- Calibrated confidence scores for each field
- Comprehensive reasoning for every data point

**Object Mode Basis**: Citations mapped to your predefined schema fields

## Architecture

- **TaskManager DO**: Main durable object storing tasks, events, and results in SQLite
- **TaskRunner DO**: Handles individual task creation and SSE streaming (one instance per task)
- **Static HTML**: Clean interface for task creation and monitoring

## Database Schema

```sql
-- Task metadata and status
CREATE TABLE tasks (
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
);

-- All SSE events received for each task
CREATE TABLE task_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_data TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  FOREIGN KEY (task_id) REFERENCES tasks (id)
);
```

## API Endpoints

- `POST /api/tasks`: Create new task with chosen schema mode
- `GET /api/tasks`: List all tasks with status and metadata
- `GET /task/{id}`: HTML page showing complete task details and event history

## Usage Examples

### Text Mode

```json
{
  "apiKey": "your-key",
  "processor": "base",
  "input": "Tesla",
  "taskSpec": {
    "output_schema": {
      "type": "text",
      "description": "Brief company overview including history and main products"
    }
  }
}
```

### Auto Mode

```json
{
  "apiKey": "your-key",
  "processor": "ultra",
  "input": "Comprehensive analysis of Tesla including financials, competitors, and market position",
  "taskSpec": {
    "output_schema": {
      "type": "auto"
    }
  }
}
```

### Object Mode

```json
{
  "apiKey": "your-key",
  "processor": "core",
  "input": "Tesla",
  "taskSpec": {
    "output_schema": {
      "type": "json",
      "json_schema": {
        "type": "object",
        "properties": {
          "company_name": {
            "type": "string",
            "description": "Full company name"
          },
          "ceo": { "type": "string", "description": "Current CEO name" },
          "stock_ticker": { "type": "string", "description": "Stock symbol" }
        },
        "required": ["company_name", "ceo", "stock_ticker"],
        "additionalProperties": false
      }
    }
  }
}
```

All modes support real-time SSE streaming for progress monitoring and provide detailed basis information for verification and transparency.
