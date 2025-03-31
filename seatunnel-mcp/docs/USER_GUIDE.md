# SeaTunnel MCP User Guide

This guide explains how to use the SeaTunnel Model Context Protocol (MCP) server with Claude and other LLM interfaces.

## Overview

The SeaTunnel MCP server provides a way for Large Language Models (LLMs) like Claude to interact with a SeaTunnel cluster. It enables you to:

- Submit and manage SeaTunnel jobs
- Monitor job status and system health
- Configure and manage SeaTunnel settings

## Setting Up

1. Install the SeaTunnel MCP server:
   ```bash
   git clone <repository_url>
   cd seatunnel-mcp
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -e .
   ```

2. Configure environment variables by copying `.env.example` to `.env` and modifying as needed:
   ```bash
   cp .env.example .env
   # Edit .env to set your SeaTunnel API URL and other settings
   ```

3. Start the MCP server:
   ```bash
   python -m src.seatunnel_mcp
   ```

## Using with Claude Desktop

To use the SeaTunnel MCP server with Claude Desktop:

1. Add the following to your `claude_desktop_config.json`:
   ```json
   {
     "mcpServers": {
       "seatunnel": {
         "command": "python",
         "args": ["-m", "src.seatunnel_mcp"]
       }
     }
   }
   ```

2. Restart Claude Desktop for the changes to take effect.

## Available Tools

When interacting with Claude, you can use these tools through natural language:

### Connection Management

- **Get connection settings**: Ask Claude to show the current SeaTunnel connection settings.
  
  Example: "What are the current SeaTunnel connection settings?"

- **Update connection settings**: Ask Claude to update the connection to a different SeaTunnel instance.
  
  Example: "Change the SeaTunnel connection to http://new-server:8090 with API key 'my_new_key'"

### Job Management

- **Submit a job**: Ask Claude to submit a new SeaTunnel job.
  
  Example: 
  ```
  Please submit this job to SeaTunnel:
  
  env {
    job.mode = "batch"
  }
  
  source {
    Jdbc {
      url = "jdbc:hive2://host:10000/default"
      query = "select * from test limit 100"
    }
  }
  
  sink {
    Elasticsearch {
      hosts = ["http://elastic:9200"]
      index = "my-index"
    }
  }
  ```

- **Stop a job**: Ask Claude to stop a running job.
  
  Example: "Please stop job with ID 12345"

- **Get job information**: Ask Claude to show information about a specific job.
  
  Example: "Show me details about job 12345"

- **Get running jobs**: Ask Claude to list all running jobs.
  
  Example: "What jobs are currently running?"

- **Get finished jobs**: Ask Claude to list finished jobs by state.
  
  Example: "Show me all completed jobs" or "List all failed jobs"

### System Monitoring

- **Get cluster overview**: Ask Claude to show an overview of the SeaTunnel cluster.
  
  Example: "Give me an overview of the SeaTunnel cluster"

- **Get system monitoring information**: Ask Claude to show detailed system monitoring information.
  
  Example: "Show me the system monitoring information"

## Example Conversations

Here are some example conversations with Claude using the SeaTunnel MCP server:

### Submitting a Job

**You**: I need to transfer data from a Hive table to Elasticsearch. Can you help me submit a job for this?

**Claude**: I can help you submit a job to transfer data from Hive to Elasticsearch. Let me walk you through creating and submitting a job configuration.

First, let me get the current connection settings to make sure we're connected to the right SeaTunnel instance.

[Claude gets connection settings]

We're currently connected to [SeaTunnel URL]. Now, let's create and submit a job configuration.

Here's a job I can submit for you that will transfer data from a Hive table to Elasticsearch. I'll need a few details:

1. The Hive connection URL
2. Which table you want to query
3. The Elasticsearch host URL
4. The target index name

Based on the information you've provided, I'll submit this job:

[Claude submits job with proper configuration]

The job has been submitted successfully! The job ID is [job ID]. You can monitor its status by asking me about this job's status.

### Monitoring Jobs

**You**: What jobs are currently running?

**Claude**: Let me check the currently running jobs for you.

[Claude gets running jobs]

Here are the currently running jobs in your SeaTunnel cluster:

[Claude displays job information in a formatted table]

Would you like to see more details about any specific job?

## Troubleshooting

If you encounter issues:

1. Check that the SeaTunnel API is accessible at the configured URL
2. Verify that any API keys are correct
3. Check the MCP server logs for error messages
4. Ensure the job configurations follow the correct format

## Advanced Usage

### Custom Connection Settings

You can configure multiple SeaTunnel connections by asking Claude to update the connection settings:

"Connect to our production SeaTunnel at https://prod-seatunnel:8090 with API key 'prod_key'"

"Connect to our development SeaTunnel at https://dev-seatunnel:8090"

### Job Configuration Templates

You can ask Claude to save and reuse job templates:

"Remember this job configuration as 'hive-to-elastic' template:
```
env {
  job.mode = "batch"
}
source {
  Jdbc {
    url = "jdbc:hive2://host:10000/default"
    query = "select * from ${table} limit ${limit}"
  }
}
sink {
  Elasticsearch {
    hosts = ["http://elastic:9200"]
    index = "${index}"
  }
}
```"

Then later: "Submit a job using the 'hive-to-elastic' template with table='users', limit=1000, and index='users-index'" 