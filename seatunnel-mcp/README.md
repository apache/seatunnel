# SeaTunnel MCP Server

A Model Context Protocol (MCP) server for interacting with SeaTunnel through LLM interfaces like Claude.

![SeaTunnel MCP Logo](./docs/img/seatunnel-mcp-logo.png)

![SeaTunnel MCP Server](./docs/img/img.png)

## Operation Video

To help you better understand the features and usage of SeaTunnel MCP, we provide a video demonstration. Please refer to the link below or directly check the video file in the project documentation directory.

https://youtu.be/bA91Vc8WGR8

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/bA91Vc8WGR8/0.jpg)](https://www.youtube.com/watch?v=bA91Vc8WGR8)


> **Tip**: If the video does not play directly, make sure your device supports MP4 format and try opening it with a modern browser or video player. 


## Features

* Job management (submit, stop, monitor)
* System monitoring and information retrieval
* REST API interaction with SeaTunnel services
* Built-in logging and monitoring tools
* Dynamic connection configuration
* Comprehensive job information and statistics

## Installation

```bash
# Clone repository
git clone <repository_url>
cd seatunnel-mcp

# Create virtual environment and install
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

## Requirements

* Python ≥ 3.9
* Running SeaTunnel instance
* Node.js (for testing with MCP Inspector)

## Usage

### Environment Variables

```
SEATUNNEL_API_URL=http://localhost:8090  # Default SeaTunnel REST API URL
SEATUNNEL_API_KEY=your_api_key           # Optional: Default SeaTunnel API key
```

### Dynamic Connection Configuration

The server provides tools to view and update connection settings at runtime:

* `get-connection-settings`: View current connection URL and API key status
* `update-connection-settings`: Update URL and/or API key to connect to a different SeaTunnel instance

Example usage through MCP:

```json
// Get current settings
{
  "name": "get-connection-settings"
}

// Update connection settings
{
  "name": "update-connection-settings",
  "arguments": {
    "url": "http://new-host:8090",
    "api_key": "new-api-key"
  }
}
```

### Job Management

The server provides tools to submit and manage SeaTunnel jobs:

* `submit-job`: Submit a new job with job configuration
* `stop-job`: Stop a running job
* `get-job-info`: Get detailed information about a specific job
* `get-running-jobs`: List all currently running jobs
* `get-finished-jobs`: List all finished jobs by state (FINISHED, CANCELED, FAILED, etc.)

### Running the Server

```bash
python -m src.seatunnel_mcp
```

### Usage with Claude Desktop

To use this with Claude Desktop, add the following to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "seatunnel": {
      "command": "python",
      "args": ["-m", "src.seatunnel_mcp"],
      "cwd": "Project root directory"
    }
  }
}
```

### Testing with MCP Inspector

```bash
npx @modelcontextprotocol/inspector python -m src.seatunnel_mcp
```

## Available Tools

### Connection Management

* `get-connection-settings`: View current SeaTunnel connection URL and API key status
* `update-connection-settings`: Update URL and/or API key to connect to a different instance

### Job Management

* `submit-job`: Submit a new job with configuration in HOCON format
* `stop-job`: Stop a running job with optional savepoint
* `get-job-info`: Get detailed information about a specific job
* `get-running-jobs`: List all currently running jobs
* `get-running-job`: Get details about a specific running job
* `get-finished-jobs`: List all finished jobs by state

### System Monitoring

* `get-overview`: Get an overview of the SeaTunnel cluster
* `get-system-monitoring-information`: Get detailed system monitoring information

## Contributing

1. Fork repository
2. Create feature branch
3. Commit changes
4. Create pull request

## License

Apache License 