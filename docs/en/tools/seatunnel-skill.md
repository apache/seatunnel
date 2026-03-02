---
sidebar_position: 2
---

# SeaTunnel Skill

SeaTunnel Skill is a Claude Code AI integration that provides instant assistance for SeaTunnel operations, configuration, and troubleshooting.

## Features

- **AI-Powered Assistant**: Get instant help with SeaTunnel concepts and configurations
- **Knowledge Integration**: Query official documentation and best practices
- **Smart Debugging**: Analyze errors and suggest fixes
- **Code Examples**: Generate configuration examples for your use case

## Installation

```bash
# Clone the repository
git clone https://github.com/apache/seatunnel-tools.git
cd seatunnel-tools

# Copy the skill to Claude Code skills directory
cp -r seatunnel-skill ~/.claude/skills/
```

## Usage

After installation, use the skill in Claude Code:

```bash
# Query SeaTunnel documentation
/seatunnel-skill "How do I configure a MySQL to PostgreSQL job?"

# Get connector information
/seatunnel-skill "List all available Kafka connector options"

# Debug configuration issues
/seatunnel-skill "Why is my job failing with OutOfMemoryError?"

# Generate configuration examples
/seatunnel-skill "Create a MySQL to Elasticsearch job config"
```

## Requirements

- [Claude Code](https://claude.ai/code) installed
- Claude Code skills directory at `~/.claude/skills/`
