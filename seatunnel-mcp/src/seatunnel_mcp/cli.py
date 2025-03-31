# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""SeaTunnel MCP 命令行界面。

为 SeaTunnel MCP 服务器提供命令行工具，便于启动、管理和配置服务器。
"""

import os
import sys
import argparse
import logging
import json
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv

from . import __version__
from .__main__ import main as run_server


def setup_logging(level: str) -> None:
    """设置日志级别。
    
    Args:
        level: 日志级别 (debug, info, warning, error, critical)
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"无效的日志级别: {level}")
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def print_version() -> None:
    """打印版本信息。"""
    print(f"SeaTunnel MCP 版本: {__version__}")


def create_env_file(env_file: str) -> None:
    """创建环境变量配置文件。
    
    Args:
        env_file: 环境变量文件路径
    """
    if os.path.exists(env_file):
        print(f"错误: 文件已存在: {env_file}")
        sys.exit(1)
    
    example_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env.example")
    
    if os.path.exists(example_file):
        with open(example_file, "r") as src, open(env_file, "w") as dst:
            dst.write(src.read())
        print(f"已创建环境变量文件: {env_file}")
    else:
        # 如果找不到示例文件，创建一个基本的配置
        with open(env_file, "w") as f:
            f.write("# SeaTunnel MCP 服务器配置\n\n")
            f.write("# MCP 服务器配置\n")
            f.write("MCP_HOST=127.0.0.1\n")
            f.write("MCP_PORT=8080\n\n")
            f.write("# SeaTunnel API 配置\n")
            f.write("SEATUNNEL_API_URL=http://localhost:8090\n")
            f.write("SEATUNNEL_API_KEY=your_api_key_here\n")
        print(f"已创建基本环境变量文件: {env_file}")


def configure_mcp_for_claude_desktop(config_file: Optional[str] = None) -> None:
    """为 Claude Desktop 配置 MCP 服务器。
    
    Args:
        config_file: Claude Desktop 配置文件路径，如果未提供则使用默认路径
    """
    if config_file is None:
        # 尝试找到默认配置文件
        home_dir = os.path.expanduser("~")
        default_paths = [
            os.path.join(home_dir, ".claude", "claude_desktop_config.json"),
            os.path.join(home_dir, "AppData", "Roaming", "claude", "claude_desktop_config.json"),
            os.path.join(home_dir, "Library", "Application Support", "claude", "claude_desktop_config.json"),
        ]
        
        for path in default_paths:
            if os.path.exists(path):
                config_file = path
                break
        
        if config_file is None:
            print("错误: 无法找到 Claude Desktop 配置文件")
            print("请手动指定配置文件路径: seatunnel-mcp configure-claude --config-file PATH")
            sys.exit(1)
    
    # 读取现有配置或创建新配置
    config = {}
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
        except json.JSONDecodeError:
            print(f"警告: {config_file} 包含无效的 JSON，将创建新配置")
    
    # 添加 SeaTunnel MCP 配置
    if "mcpServers" not in config:
        config["mcpServers"] = {}
    
    config["mcpServers"]["seatunnel"] = {
        "command": "python",
        "args": ["-m", "src.seatunnel_mcp"]
    }
    
    # 保存配置
    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    with open(config_file, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"已为 Claude Desktop 配置 SeaTunnel MCP 服务器: {config_file}")


def main() -> None:
    """命令行入口点。"""
    parser = argparse.ArgumentParser(description="SeaTunnel MCP 服务器命令行工具")
    
    # 全局选项
    parser.add_argument("-v", "--version", action="store_true", help="显示版本信息")
    parser.add_argument("--log-level", choices=["debug", "info", "warning", "error", "critical"],
                      default="info", help="设置日志级别 (默认: info)")
    
    # 子命令
    subparsers = parser.add_subparsers(dest="command", help="命令")
    
    # 运行服务器
    run_parser = subparsers.add_parser("run", help="运行 MCP 服务器")
    run_parser.add_argument("--host", help="监听主机 (默认: 从环境变量获取)")
    run_parser.add_argument("--port", type=int, help="监听端口 (默认: 从环境变量获取)")
    run_parser.add_argument("--api-url", help="SeaTunnel API URL (默认: 从环境变量获取)")
    run_parser.add_argument("--api-key", help="SeaTunnel API 密钥 (默认: 从环境变量获取)")
    run_parser.add_argument("--env-file", help="环境变量文件路径 (默认: .env)")
    
    # 初始化环境变量文件
    init_parser = subparsers.add_parser("init", help="初始化环境变量文件")
    init_parser.add_argument("--env-file", default=".env", help="环境变量文件路径 (默认: .env)")
    
    # 为 Claude Desktop 配置 MCP
    claude_parser = subparsers.add_parser("configure-claude", help="为 Claude Desktop 配置 MCP 服务器")
    claude_parser.add_argument("--config-file", help="Claude Desktop 配置文件路径")
    
    args = parser.parse_args()
    
    # 设置日志级别
    setup_logging(args.log_level)
    
    # 显示版本信息
    if args.version:
        print_version()
        return
    
    # 处理命令
    if args.command == "run":
        # 加载环境变量
        if args.env_file:
            load_dotenv(args.env_file)
        else:
            load_dotenv()
        
        # 设置环境变量
        if args.host:
            os.environ["MCP_HOST"] = args.host
        if args.port:
            os.environ["MCP_PORT"] = str(args.port)
        if args.api_url:
            os.environ["SEATUNNEL_API_URL"] = args.api_url
        if args.api_key:
            os.environ["SEATUNNEL_API_KEY"] = args.api_key
        
        # 运行服务器
        run_server()
    
    elif args.command == "init":
        create_env_file(args.env_file)
    
    elif args.command == "configure-claude":
        configure_mcp_for_claude_desktop(args.config_file)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 