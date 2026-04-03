---
sidebar_position: 2
---

# SeaTunnel Skill

SeaTunnel Skill 是 Claude Code 的 AI 集成技能，为 SeaTunnel 的操作、配置和故障排查提供即时帮助。

## 功能特性

- **AI 助手**：即时获取 SeaTunnel 概念和配置相关帮助
- **知识集成**：查询官方文档和最佳实践
- **智能调试**：分析错误并给出修复建议
- **代码示例**：为您的用例自动生成配置示例

## 安装

```bash
# 克隆仓库
git clone https://github.com/apache/seatunnel-tools.git
cd seatunnel-tools

# 复制技能文件到 Claude Code 技能目录
cp -r seatunnel-skill ~/.claude/skills/
```

## 使用方法

安装完成后，在 Claude Code 中使用：

```bash
# 查询 SeaTunnel 文档
/seatunnel-skill "如何配置 MySQL 到 PostgreSQL 的数据同步？"

# 获取连接器信息
/seatunnel-skill "列出所有可用的 Kafka 连接器选项"

# 调试配置问题
/seatunnel-skill "为什么我的任务出现 OutOfMemoryError 错误？"

# 生成配置示例
/seatunnel-skill "创建一个 MySQL 到 Elasticsearch 的任务配置"
```

## 系统要求

- 已安装 [Claude Code](https://claude.ai/code)
- Claude Code 技能目录位于 `~/.claude/skills/`
