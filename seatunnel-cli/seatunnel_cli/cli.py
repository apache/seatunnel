#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""SeaTunnel CLI - Interactive terminal interface."""

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.markdown import Markdown
from rich.panel import Panel
from rich.spinner import Spinner
from rich.syntax import Syntax
from rich.text import Text
from rich.theme import Theme

from prompt_toolkit import prompt as pt_prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory

from . import __version__, get_data_dir
from .llm_provider import create_provider
from .agents import Orchestrator


# ─── Theme ───

THEME = Theme({
    "info": "cyan",
    "success": "bold green",
    "warning": "yellow",
    "error": "bold red",
    "heading": "bold magenta",
})

BANNER = r"""
  ____             _____                        _
 / ___|  ___  __ _|_   _|   _ _ __  _ __   ___ | |
 \___ \ / _ \/ _` | | || | | | '_ \| '_ \ / _ \| |
  ___) |  __/ (_| | | || |_| | | | | | | |  __/| |
 |____/ \___|\__,_| |_| \__,_|_| |_|_| |_|\___||_|

"""

WELCOME = f"""[heading]SeaTunnel CLI v{__version__}[/heading]
Generate Apache SeaTunnel configs with natural language.

[info]Commands:[/info]
  Type your request in natural language (Chinese or English)
  [bold]/save <path>[/bold]     — Save config to custom path (auto-saved to .data/last_job.conf)
  [bold]/check[/bold]           — Dry-run validate last config (auto-fixes on failure)
  [bold]/run[/bold]             — Execute last config with SeaTunnel
  [bold]/connectors[/bold]      — List available connectors
  [bold]/config[/bold]          — Show/change LLM provider settings
  [bold]/sessions[/bold]        — List recent sessions
  [bold]/resume [id][/bold]     — Resume a previous session
  [bold]/new[/bold]             — Start a fresh session
  [bold]/memory[/bold]          — Show remembered facts
  [bold]/remember <text>[/bold] — Save a fact to memory
  [bold]/forget <id|--all>[/bold] — Delete a memory entry (or all)
  [bold]/clear[/bold]           — Clear conversation history
  [bold]/help[/bold]            — Show this help
  [bold]/quit[/bold]            — Exit
"""


# ─── Credential placeholder helpers for config repair ───

_CRED_KV_RE = re.compile(
    r'((?:password|passwd|secret[-_]?key|access[-_]?key|api[-_]?key|token|'
    r'auth[-_]?token|secret|credential|private[-_]?key)'
    r'\s*=\s*)"([^"]*)"',
    re.IGNORECASE,
)


def _replace_creds_with_placeholders(config: str) -> tuple[str, dict[str, str]]:
    """Replace credential values with ${_CRED_N_} placeholders.

    Returns (safe_config, mapping) where mapping can restore originals.
    """
    cred_map: dict[str, str] = {}
    counter = [0]

    def _replacer(m: re.Match) -> str:
        key_part = m.group(1)  # e.g. 'password = '
        value = m.group(2)
        if value.startswith("${"):
            return m.group(0)  # Already a placeholder, skip
        counter[0] += 1
        placeholder = f"${{_CRED_{counter[0]}_}}"
        cred_map[placeholder] = value
        return f'{key_part}"{placeholder}"'

    safe = _CRED_KV_RE.sub(_replacer, config)
    return safe, cred_map


def _restore_creds_from_placeholders(config: str, cred_map: dict[str, str]) -> str:
    """Restore original credential values from ${_CRED_N_} placeholders."""
    for placeholder, original in cred_map.items():
        config = config.replace(placeholder, original)
    return config


class SeaTunnelCLI:
    """Interactive CLI for SeaTunnel config generation."""

    def __init__(self, console: Console):
        self.console = console
        self.client = None  # initialized lazily; set by _ensure_provider()
        self.history_dir = get_data_dir()

        from .memory import SessionManager, MemoryStore
        self.session_manager = SessionManager(self.history_dir)
        self.memory_store = MemoryStore(self.history_dir)

        self.orchestrator = None  # initialized after provider is ready
        self.last_config: str | None = None
        self._pending_request: str | None = None  # original request awaiting clarification
        self.status_text = ""
        self._live: Live | None = None
        self._stream_buffer = ""
        self._stream_mode: str | None = None
        self._streamed_chat = False
        self._interaction_count = 0

    def _ensure_provider(self) -> bool:
        """Try to create LLM provider and orchestrator.

        Returns True if ready, False if no provider could be created.
        """
        if self.client and self.orchestrator:
            return True
        try:
            self.client = create_provider()
            self.orchestrator = Orchestrator(
                client=self.client,
                on_status=self._show_status,
                on_stream=self._handle_stream,
                memory_store=self.memory_store,
            )
            return True
        except (ValueError, ImportError):
            return False

    def _show_status(self, phase: str, message: str):
        """Display status updates during agent processing."""
        self._stop_live()
        icons = {
            "thinking": "🧠",
            "generating": "⚙️ ",
            "validating": "✅",
            "fixing": "🔧",
        }
        icon = icons.get(phase, "⏳")
        self.status_text = f"{icon} {message}"
        self.console.print(f"  {icon} {message}", style="info")

    def _handle_stream(self, tag: str, event: dict):
        """Handle streaming events from the agent pipeline."""
        etype = event.get("type", "")

        if etype == "text_delta":
            self._stream_buffer += event.get("text", "")

            if self._live is None:
                self._stream_mode = tag
                if tag == "chat":
                    self._streamed_chat = True
                self._live = Live(
                    console=self.console,
                    refresh_per_second=8,
                    transient=(tag == "config"),
                )
                self._live.start()

            if tag == "chat":
                self._live.update(
                    Panel(
                        Markdown(self._stream_buffer),
                        title="🐬 SeaTunnel",
                        border_style="cyan",
                        padding=(1, 2),
                    )
                )
            elif tag == "config":
                self._live.update(
                    Panel(
                        Text(self._stream_buffer, style="dim"),
                        title="⚙️  Generating...",
                        border_style="yellow",
                        padding=(1, 2),
                    )
                )

        elif etype == "message_stop":
            self._stop_live()

    def _stop_live(self):
        """Stop the Live display if active."""
        if self._live is not None:
            self._live.stop()
            self._live = None
        self._stream_buffer = ""
        self._stream_mode = None

    # ─── Session & Memory ───

    def _init_session(self):
        """Resume last session if recent, otherwise start new."""
        latest = self.session_manager.get_latest_session_id()
        if latest:
            try:
                loaded, last_config = self.session_manager.load_session(latest)
                if loaded:
                    self.orchestrator.load_history(loaded)
                    self.last_config = last_config
                    self.console.print(
                        f"  Session: [bold]{latest}[/bold] resumed ({len(loaded)} messages)",
                        style="info",
                    )
                    return
            except Exception:
                pass
        sid = self.session_manager.new_session()
        self.console.print(f"  Session: [bold]{sid}[/bold] (new)", style="info")

        mem_count = len(self.memory_store.get_all())
        if mem_count:
            self.console.print(f"  Memory:  [bold]{mem_count}[/bold] remembered facts", style="info")

    def _save_and_exit(self):
        if self.orchestrator.conversation_history:
            self.session_manager.save_session(
                self.orchestrator.conversation_history,
                last_config=self.last_config,
            )
            try:
                summary = self.session_manager.generate_summary(
                    self.orchestrator.conversation_history, self.client
                )
                self.session_manager.update_summary(summary)
            except Exception:
                pass
        self.console.print("Bye! 👋", style="info")

    def _maybe_extract_memories(self, result: dict | None):
        if result is None:
            return
        should_extract = (
            result.get("type") == "config"
            or (result.get("type") == "chat" and self._interaction_count % 5 == 0)
        )
        if not should_extract:
            return
        try:
            from .memory import extract_memories
            new_facts = extract_memories(
                self.orchestrator.conversation_history,
                self.memory_store.get_all(),
                self.client,
            )
            for fact in new_facts:
                mem_id = self.memory_store.add(
                    content=fact["content"],
                    memory_type=fact["type"],
                    source="auto",
                )
                if mem_id is not None:
                    self.console.print(f"  [dim]💾 Remembered: {fact['content'][:80]} ({mem_id})[/dim]")
        except Exception:
            pass

        # Auto-compact when memories grow too large
        try:
            if self.memory_store.needs_compaction():
                before = len(self.memory_store.get_all())
                removed = self.memory_store.compact(self.client)
                if removed > 0:
                    after = len(self.memory_store.get_all())
                    self.console.print(
                        f"  [dim]🗜️ Memory compacted: {before} → {after} entries[/dim]"
                    )
        except Exception:
            pass

    def _cmd_sessions(self):
        sessions = self.session_manager.list_sessions(limit=10)
        if not sessions:
            self.console.print("  No sessions found.", style="info")
            return
        self.console.print("\n[heading]Recent Sessions:[/heading]")
        for s in sessions:
            marker = " [bold green]*[/bold green]" if s["session_id"] == self.session_manager.current_session_id else ""
            summary = s.get("summary", "") or "[dim]no summary[/dim]"
            self.console.print(
                f"  [bold]{s['session_id']}[/bold]{marker}  "
                f"({s['message_count']} msgs)  {summary}"
            )

    def _cmd_resume(self, session_id: str):
        if not session_id:
            sessions = self.session_manager.list_sessions(limit=5)
            for s in sessions:
                if s["session_id"] != self.session_manager.current_session_id:
                    session_id = s["session_id"]
                    break
            if not session_id:
                self.console.print("  No other sessions to resume.", style="warning")
                return
        try:
            self.session_manager.save_session(
                self.orchestrator.conversation_history, last_config=self.last_config
            )
            loaded, last_config = self.session_manager.load_session(session_id)
            self.orchestrator.load_history(loaded)
            self.last_config = last_config
            self.console.print(
                f"  Resumed [bold]{session_id}[/bold] ({len(loaded)} messages)", style="success"
            )
        except FileNotFoundError:
            self.console.print(f"  Session not found: {session_id}", style="error")

    def _cmd_new_session(self):
        if self.orchestrator.conversation_history:
            self.session_manager.save_session(
                self.orchestrator.conversation_history, last_config=self.last_config
            )
            try:
                summary = self.session_manager.generate_summary(
                    self.orchestrator.conversation_history, self.client
                )
                self.session_manager.update_summary(summary)
            except Exception:
                pass
        self.orchestrator.conversation_history.clear()
        self.last_config = None
        sid = self.session_manager.new_session()
        self.console.print(f"  New session: [bold]{sid}[/bold]", style="success")

    def _cmd_memory(self):
        from .memory import MemoryStore
        memories = self.memory_store.get_all()
        if not memories:
            self.console.print("  No memories. Use /remember <text> to add one.", style="info")
            return
        self.console.print("\n[heading]Memories:[/heading]")
        for mtype in MemoryStore.MEMORY_TYPES:
            typed = [m for m in memories if m["type"] == mtype]
            if typed:
                self.console.print(f"\n  [bold]{mtype.title()}:[/bold]")
                for m in typed:
                    src = " [dim](auto)[/dim]" if m["source"] == "auto" else ""
                    self.console.print(f"    [{m['id']}] {m['content']}{src}")

    def _cmd_remember(self, text: str):
        if not text:
            self.console.print("  Usage: /remember <fact>", style="warning")
            return
        memory_type = self._classify_memory(text)
        mem_id = self.memory_store.add(content=text, memory_type=memory_type, source="explicit")
        if mem_id is None:
            self.console.print(
                "  [error]Rejected:[/error] Memory contains credentials (password, API key, token, etc.).\n"
                "  Credentials are never stored in memory for security reasons.\n"
                "  Tip: Store only non-secret facts (host, port, database name).",
                style="warning",
            )
            return
        self.console.print(
            f"  Saved [bold]{mem_id}[/bold] (type: {memory_type})", style="success"
        )

    def _cmd_forget(self, memory_id: str):
        if not memory_id:
            self.console.print("  Usage: /forget <memory-id> or /forget --all", style="warning")
            return
        if memory_id == "--all":
            count = self.memory_store.clear()
            if count:
                self.console.print(f"  Cleared all {count} memories.", style="success")
            else:
                self.console.print("  No memories to clear.", style="info")
            return
        if self.memory_store.remove(memory_id):
            self.console.print(f"  Removed {memory_id}.", style="success")
        else:
            self.console.print(f"  Not found: {memory_id}", style="error")

    @staticmethod
    def _classify_memory(text: str) -> str:
        text_lower = text.lower()
        if any(kw in text_lower for kw in [
            "host", "port", "url", "jdbc", "broker", "server",
            "user=", "password", "database", ":3306", ":5432",
            ":9092", ":8030", ":8123", "connection",
        ]):
            return "connection"
        if any(kw in text_lower for kw in [
            "prefer", "always", "default", "language", "chinese",
            "english", "parallelism", "format",
        ]):
            return "preference"
        return "project"

    def _cmd_config(self, arg: str):
        """Show or change provider configuration."""
        from .llm_provider import load_config, save_config, _PROVIDERS

        if not arg:
            # Show current config
            config = load_config()
            self.console.print("\n[heading]LLM Provider Configuration:[/heading]")
            self.console.print(f"  Current provider: [bold]{self.client.provider_name}[/bold]")
            self.console.print(f"  Model:            [bold]{self.client.model_id}[/bold]")
            self.console.print(f"  Fast model:       [bold]{self.client.fast_model_id}[/bold]")
            from .llm_provider import get_config_path
            config_file = get_config_path()
            if Path(config_file).exists():
                self.console.print(f"  Config file:      [bold]{config_file}[/bold]")
            else:
                self.console.print(f"  Config file:      [dim]not set (using env/auto-detect)[/dim]")
            self.console.print(f"\n  [dim]Change provider: /config <{', '.join(sorted(_PROVIDERS.keys()))}>[/dim]")
            return

        provider_name = arg.lower()
        if provider_name not in _PROVIDERS:
            self.console.print(
                f"  Unknown provider: {arg}. Available: {', '.join(sorted(_PROVIDERS.keys()))}",
                style="error",
            )
            return

        # Save to config file
        config = load_config()
        config["provider"] = provider_name
        save_config(config)

        # Try to create the new provider
        try:
            from .llm_provider import create_provider
            new_client = create_provider(provider_name)
            self.client = new_client
            self.orchestrator.client = new_client
            self.console.print(
                f"  Provider switched to [bold]{provider_name}[/bold]", style="success"
            )
            self.console.print(f"  Model: [bold]{new_client.model_id}[/bold]", style="info")
            self.console.print(f"  Saved to {get_config_path()}", style="info")
        except Exception as e:
            self.console.print(f"  Failed to init {provider_name}: {e}", style="error")
            self.console.print(
                "  Config saved — provider will be used on next start if credentials are set.",
                style="info",
            )

    @staticmethod
    def run_init(console: Console):
        """Interactive first-time setup. Guides user through provider selection and credentials."""
        from .llm_provider import load_config, save_config, _PROVIDERS, _auto_detect_provider

        console.print("\n[heading]SeaTunnel CLI — Initial Setup[/heading]\n")

        # Security notice
        console.print(
            Panel(
                "[bold]Security Notice:[/bold]\n"
                "- API keys are NEVER stored on disk — only in environment variables\n"
                "- You can enter a key below for this session, or set it in your shell profile\n"
                "- config.json only stores provider name, model IDs, and non-secret settings\n"
                "- Passwords/secrets in generated configs use ${ENV_VAR} placeholders",
                border_style="yellow",
                padding=(0, 2),
            )
        )

        # Show detected credentials
        detected = _auto_detect_provider()
        if detected:
            console.print(f"  Auto-detected credentials for: [bold]{detected}[/bold]\n")

        # ── Step 1: Provider selection (no default — user must choose) ──
        console.print("  [bold]Step 1:[/bold] Choose your LLM provider\n")
        console.print("    [bold]1[/bold]. anthropic  — Anthropic API (Claude)")
        console.print("       Requires: ANTHROPIC_API_KEY")
        console.print("       Get a key: https://console.anthropic.com/settings/keys\n")
        console.print("    [bold]2[/bold]. openai     — OpenAI or any compatible API (GPT, DeepSeek, etc.)")
        console.print("       Requires: OPENAI_API_KEY (and optionally OPENAI_BASE_URL)")
        console.print("       Get a key: https://platform.openai.com/api-keys\n")
        console.print("    [bold]3[/bold]. bedrock    — AWS Bedrock (Claude via AWS)")
        console.print("       Requires: AWS credentials (aws configure / env vars / IAM role)")
        console.print("       Docs: https://docs.aws.amazon.com/bedrock/\n")

        try:
            choice = pt_prompt("  Enter your choice (1/2/3): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("\n  Setup cancelled.", style="warning")
            return

        choice_map = {"1": "anthropic", "2": "openai", "3": "bedrock"}
        choice = choice_map.get(choice, choice)

        if not choice or choice not in _PROVIDERS:
            console.print(
                f"  [error]Invalid choice: '{choice}'. Please enter 1, 2, or 3.[/error]"
            )
            return

        # Build config
        config = load_config()
        config["provider"] = choice

        # ── Step 2: Credentials ──
        console.print(f"\n  [bold]Step 2:[/bold] Configure credentials for [bold]{choice}[/bold]\n")

        if choice == "anthropic":
            existing = os.environ.get("ANTHROPIC_API_KEY")
            if existing:
                masked = existing[:7] + "..." + existing[-4:] if len(existing) > 15 else "***"
                console.print(f"  ANTHROPIC_API_KEY: [bold green]detected[/bold green] ({masked})")
            else:
                console.print("  ANTHROPIC_API_KEY not found in environment.\n")
                console.print(
                    "  [dim]Persistent (recommended): add to ~/.zshrc or ~/.bashrc:[/dim]\n"
                    "    export ANTHROPIC_API_KEY=sk-ant-...\n",
                )
                try:
                    key_input = pt_prompt(
                        "  Enter API key for this session (or Enter to skip): ",
                    ).strip()
                except (EOFError, KeyboardInterrupt):
                    key_input = ""
                if key_input:
                    os.environ["ANTHROPIC_API_KEY"] = key_input
                    console.print(
                        "  [success]Key set for this session (NOT saved to disk).[/success]"
                    )

        elif choice == "openai":
            existing = os.environ.get("OPENAI_API_KEY")
            if existing:
                masked = existing[:7] + "..." + existing[-4:] if len(existing) > 15 else "***"
                console.print(f"  OPENAI_API_KEY: [bold green]detected[/bold green] ({masked})")
            else:
                console.print("  OPENAI_API_KEY not found in environment.\n")
                console.print(
                    "  [dim]Persistent (recommended): add to ~/.zshrc or ~/.bashrc:[/dim]\n"
                    "    export OPENAI_API_KEY=sk-...\n",
                )
                try:
                    key_input = pt_prompt(
                        "  Enter API key for this session (or Enter to skip): ",
                    ).strip()
                except (EOFError, KeyboardInterrupt):
                    key_input = ""
                if key_input:
                    os.environ["OPENAI_API_KEY"] = key_input
                    console.print(
                        "  [success]Key set for this session (NOT saved to disk).[/success]"
                    )

            # Base URL for compatible APIs
            existing_url = os.environ.get("OPENAI_BASE_URL")
            if existing_url:
                console.print(f"  OPENAI_BASE_URL: [bold]{existing_url}[/bold]")
            else:
                console.print(
                    "\n  [dim]Using a compatible API (Azure OpenAI, DeepSeek, local model, etc.)?[/dim]"
                )
                try:
                    base_url = pt_prompt(
                        "  Enter base URL (or Enter to use default OpenAI endpoint): ",
                    ).strip()
                except (EOFError, KeyboardInterrupt):
                    base_url = ""
                if base_url:
                    os.environ["OPENAI_BASE_URL"] = base_url
                    config.setdefault("settings", {})["openai_base_url"] = base_url
                    console.print(f"  Base URL set: [bold]{base_url}[/bold]")

        elif choice == "bedrock":
            console.print("  AWS Bedrock requires AWS credentials.\n")
            console.print(
                "  [dim]Options:[/dim]\n"
                "    - aws configure           (interactive setup)\n"
                "    - export AWS_ACCESS_KEY_ID=... && export AWS_SECRET_ACCESS_KEY=...\n"
                "    - IAM role (EC2/ECS)      (automatic)\n"
                "    - export AWS_PROFILE=...  (named profile)\n"
            )
            try:
                import boto3
                sts = boto3.client("sts")
                identity = sts.get_caller_identity()
                console.print(
                    f"  AWS credentials: [bold green]valid[/bold green] "
                    f"(Account: {identity.get('Account', 'unknown')})"
                )
            except ImportError:
                console.print(
                    "  [error]boto3 not installed. Install it: pip install boto3[/error]"
                )
            except Exception:
                console.print(
                    "  [warning]AWS credentials not detected. "
                    "Run 'aws configure' or set environment variables.[/warning]"
                )

            # Region
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
            if region:
                console.print(f"  AWS Region: [bold]{region}[/bold]")
            else:
                try:
                    region_input = pt_prompt(
                        "  Enter AWS region (e.g., us-east-1): ",
                    ).strip()
                except (EOFError, KeyboardInterrupt):
                    region_input = ""
                if region_input:
                    os.environ["AWS_REGION"] = region_input
                    console.print(f"  Region set: [bold]{region_input}[/bold]")

        # ── Step 3: Model selection (optional) ──
        console.print(f"\n  [bold]Step 3:[/bold] Model selection (optional)\n")

        # Show provider-specific defaults
        if choice == "anthropic":
            default_model = "claude-sonnet-4-20250514"
            default_fast = "claude-haiku-4-5-20251001"
            model_env = "ANTHROPIC_MODEL"
            fast_env = "ANTHROPIC_SMALL_FAST_MODEL"
        elif choice == "openai":
            default_model = "gpt-4o"
            default_fast = "gpt-4o-mini"
            model_env = "OPENAI_MODEL"
            fast_env = "OPENAI_SMALL_FAST_MODEL"
        else:  # bedrock
            default_model = "us.anthropic.claude-sonnet-4-20250514-v1:0"
            default_fast = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
            model_env = "ANTHROPIC_MODEL"
            fast_env = "ANTHROPIC_SMALL_FAST_MODEL"

        console.print(f"  Primary model (for config generation):")
        console.print(f"    Default: [dim]{default_model}[/dim]")
        console.print(f"    Env var: [dim]{model_env}[/dim]")
        try:
            model_input = pt_prompt(
                f"  Enter model ID (or Enter for default): ",
            ).strip()
        except (EOFError, KeyboardInterrupt):
            model_input = ""

        console.print(f"\n  Fast model (for lightweight tasks like slot checking):")
        console.print(f"    Default: [dim]{default_fast}[/dim]")
        console.print(f"    Env var: [dim]{fast_env}[/dim]")
        try:
            fast_model_input = pt_prompt(
                f"  Enter fast model ID (or Enter for default): ",
            ).strip()
        except (EOFError, KeyboardInterrupt):
            fast_model_input = ""

        # Save model overrides to config (non-secret)
        if model_input or fast_model_input:
            config.setdefault("models", {}).setdefault(choice, {})
            if model_input:
                config["models"][choice]["model"] = model_input
            if fast_model_input:
                config["models"][choice]["fast_model"] = fast_model_input

        # ── Save & validate ──
        save_config(config)
        from .llm_provider import get_config_path
        console.print(f"\n  Config saved to: [bold]{get_config_path()}[/bold]")

        # Try to create the provider
        try:
            from .llm_provider import create_provider
            client = create_provider(choice)
            console.print(
                Panel(
                    f"Provider:   [bold]{choice}[/bold]\n"
                    f"Model:      [bold]{client.model_id}[/bold]\n"
                    f"Fast model: [bold]{client.fast_model_id}[/bold]\n"
                    f"Status:     [bold green]ready[/bold green]",
                    title="Setup Complete",
                    border_style="green",
                    padding=(0, 2),
                )
            )
        except Exception as e:
            console.print(
                Panel(
                    f"Provider:   [bold]{choice}[/bold]\n"
                    f"Status:     [bold yellow]credentials needed[/bold yellow]\n"
                    f"Error:      {e}\n\n"
                    f"Set the required environment variable and restart,\n"
                    f"or re-run: [bold]seatunnel --init[/bold]",
                    title="Setup Incomplete",
                    border_style="yellow",
                    padding=(0, 2),
                )
            )

        # SEATUNNEL_HOME hint
        from . import get_seatunnel_home
        st_home = get_seatunnel_home()
        if st_home:
            console.print(f"  SEATUNNEL_HOME: [bold green]{st_home}[/bold green]")
            if not os.environ.get("SEATUNNEL_HOME"):
                console.print("  [dim](auto-detected from package location)[/dim]")
        else:
            console.print(
                "\n  [bold]Optional:[/bold] Set SEATUNNEL_HOME for /check, /run and connector metadata:\n"
                "    export SEATUNNEL_HOME=/path/to/apache-seatunnel\n"
                "  [dim]Add to ~/.zshrc or ~/.bashrc for persistence.[/dim]"
            )

        console.print()

    def run_interactive(self):
        """Main interactive loop."""
        self.console.print(BANNER, style="bold cyan")
        self.console.print(Panel(WELCOME, border_style="cyan", padding=(1, 2)))

        if not self._ensure_provider():
            # No provider configured — run setup wizard
            self.console.print(
                Panel(
                    "No LLM provider configured yet.\n"
                    "Let's set one up now — it only takes a moment.",
                    title="First-Time Setup",
                    border_style="yellow",
                    padding=(1, 2),
                )
            )
            self.run_init(self.console)
            if not self._ensure_provider():
                self.console.print(
                    "\n[error]Provider still not ready. "
                    "Set the required environment variables and restart.[/error]\n"
                    "  Or re-run: [bold]seatunnel --init[/bold]",
                    style="error",
                )
                return
        self._check_env()

        self._init_session()

        history = FileHistory(str(self.history_dir / "history.txt"))

        command_completer = WordCompleter(
            ["/save", "/check", "/run", "/connectors", "/config", "/clear", "/help",
             "/quit", "/exit", "/sessions", "/resume", "/new", "/memory", "/remember", "/forget"],
            sentence=True,
        )

        while True:
            try:
                user_input = pt_prompt(
                    "\n🐬 SeaTunnel > ",
                    history=history,
                    completer=command_completer,
                ).strip()
            except (EOFError, KeyboardInterrupt):
                self._save_and_exit()
                break

            if not user_input:
                continue

            # Handle commands
            if user_input.startswith("/"):
                self._handle_command(user_input)
                continue

            # If previous turn was a clarification question, merge the
            # original request with the user's answer so the planner and
            # slot-checker see the full context.
            if self._pending_request:
                user_input = (
                    f"{self._pending_request}\n\n"
                    f"Additional details from user: {user_input}"
                )
                self._pending_request = None

            # Process natural language request
            self._process_request(user_input)

    def run_single(self, request: str, output_path: str | None = None):
        """Single-shot mode: process one request and exit."""
        if not self._ensure_provider():
            self.console.print(
                "[error]No LLM provider configured. "
                "Run 'seatunnel --init' first.[/error]"
            )
            return
        self._check_env()
        result = self._process_request(request)
        if result and result.get("config") and output_path:
            self._save_config(output_path)

    def _check_env(self) -> bool:
        """Check required environment variables and provider status.

        Returns True if credentials are valid and the CLI is ready to use.
        Returns False if credentials are missing — caller should block.
        """
        provider = self.client
        provider_name = provider.provider_name
        creds_ok = True

        self.console.print(f"  Provider: [bold]{provider_name}[/bold]", style="info")

        # Provider-specific credential checks (never log actual key values)
        if provider_name == "bedrock":
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
            if not region:
                self.console.print(
                    "[warning]Warning: AWS_REGION not set. Defaulting to us-east-1[/warning]"
                )
            try:
                import boto3
                sts = boto3.client("sts")
                identity = sts.get_caller_identity()
                self.console.print(f"  AWS Account: [bold]{identity.get('Account', 'unknown')}[/bold]", style="info")
            except Exception:
                creds_ok = False
                self.console.print(
                    "[error]AWS credentials not configured. "
                    "Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or configure AWS CLI.[/error]"
                )
        elif provider_name == "anthropic":
            if os.environ.get("ANTHROPIC_API_KEY"):
                self.console.print("  API key:  [bold green]configured[/bold green]", style="info")
            else:
                creds_ok = False
                self.console.print("[error]ANTHROPIC_API_KEY not set.[/error]")
        elif provider_name == "openai":
            if os.environ.get("OPENAI_API_KEY"):
                self.console.print("  API key:  [bold green]configured[/bold green]", style="info")
            else:
                creds_ok = False
                self.console.print("[error]OPENAI_API_KEY not set.[/error]")
            base_url = os.environ.get("OPENAI_BASE_URL")
            if base_url:
                self.console.print(f"  Base URL: [bold]{base_url}[/bold]", style="info")

        self.console.print(f"  Model: [bold]{provider.model_id}[/bold]", style="info")
        self.console.print(f"  Fast model: [bold]{provider.fast_model_id}[/bold]", style="info")
        self.console.print(
            "  [dim]Credentials are never logged or included in generated configs.[/dim]"
        )

        # Check SeaTunnel engine connection
        from .connectors import _check_engine, _ENGINE_API_BASE
        from .agents import _find_seatunnel_sh
        if _check_engine():
            self.console.print(f"  Engine: [bold green]connected[/bold green] ({_ENGINE_API_BASE})", style="info")
            self.console.print("  Mode:   [bold]cluster[/bold] (REST API available — live connector metadata)", style="info")
        else:
            self.console.print(
                f"  Engine: [bold yellow]offline[/bold yellow] ({_ENGINE_API_BASE}) — using runtime metadata",
                style="info",
            )
            self.console.print(
                "  [dim]Tip: Start SeaTunnel server (seatunnel-server.sh) for live metadata & dry-run[/dim]"
            )

        sh_path = _find_seatunnel_sh()
        if sh_path:
            self.console.print(f"  CLI:    [bold green]found[/bold green] ({sh_path})", style="info")
        else:
            self.console.print(
                "  CLI:    [bold yellow]not found[/bold yellow] — set SEATUNNEL_HOME for /run and /check",
                style="info",
            )

        # Runtime metadata status
        from .connectors import _load_runtime_metadata
        runtime_meta = _load_runtime_metadata()
        if runtime_meta:
            self.console.print(
                f"  Metadata: [bold green]{len(runtime_meta)} entries[/bold green] "
                f"(runtime JSON — 100% accurate)",
                style="info",
            )
        else:
            self.console.print(
                "  Metadata: [bold yellow]not found[/bold yellow] — "
                "using built-in defaults for common connectors",
                style="info",
            )

        self.console.print()
        return creds_ok

    def _process_request(self, user_input: str) -> dict | None:
        """Process a natural language request through the agent pipeline."""
        self.console.print()
        self._stream_buffer = ""
        self._stream_mode = None
        self._streamed_chat = False
        start_time = time.time()

        try:
            result = self.orchestrator.process_user_input(user_input)
        except Exception as e:
            self._stop_live()
            self.console.print(f"\n[error]Error: {e}[/error]")
            import traceback
            self.console.print(f"[dim]{traceback.format_exc()}[/dim]")
            return None

        self._stop_live()

        # Auto-save session and maybe extract memories
        if result and result.get("config"):
            self.last_config = result["config"]
        self.session_manager.save_session(
            self.orchestrator.conversation_history,
            last_config=self.last_config,
        )
        self._interaction_count += 1
        self._maybe_extract_memories(result)

        elapsed = time.time() - start_time

        if result["type"] == "question":
            # Store original request so next user input gets merged with it
            self._pending_request = user_input
            self.console.print()
            self.console.print(
                Panel(
                    result["content"],
                    title="🤔 Clarification needed",
                    border_style="yellow",
                    padding=(1, 2),
                )
            )
            return result

        elif result["type"] == "config":
            config = result["config"]
            explanation = result.get("explanation", "")

            self.console.print()
            self.console.print(
                Panel(
                    Syntax(config, "properties", theme="monokai", line_numbers=True),
                    title="📋 Generated SeaTunnel Config",
                    border_style="green",
                    padding=(1, 2),
                )
            )

            if explanation:
                self.console.print()
                self.console.print(
                    Panel(
                        Markdown(explanation),
                        title="💡 Explanation",
                        border_style="cyan",
                        padding=(1, 2),
                    )
                )

            dryrun = result.get("dry_run")
            if dryrun:
                if dryrun["valid"]:
                    self.console.print("  Dry-run: [bold green]PASSED[/bold green]", style="info")
                else:
                    self.console.print(
                        "  Dry-run: [bold yellow]PARTIAL[/bold yellow] — use /check for details",
                        style="info",
                    )

            saved_path = self._auto_save_config()
            self.console.print(f"\n  [dim]Generated in {elapsed:.1f}s[/dim]")
            if saved_path:
                self.console.print(f"  Config saved to: [bold]{saved_path}[/bold]", style="success")
            self.console.print()
            self.console.print("  [bold][1][/bold] /save <path> — Save to custom path")
            self.console.print("  [bold][2][/bold] /check       — Dry-run validate")
            self.console.print("  [bold][3][/bold] /run         — Execute with SeaTunnel")
            self.console.print("  [bold][4][/bold] Continue chatting to modify")
            return result

        elif result["type"] == "chat":
            if not self._streamed_chat:
                self.console.print()
                self.console.print(
                    Panel(
                        Markdown(result["content"]),
                        title="🐬 SeaTunnel",
                        border_style="cyan",
                        padding=(1, 2),
                    )
                )
            self.console.print(f"  [dim]{elapsed:.1f}s[/dim]")
            return result

        elif result["type"] == "error":
            self.console.print(f"\n[error]{result['content']}[/error]")
            return result

        return None

    def _handle_command(self, cmd: str):
        """Handle CLI commands."""
        parts = cmd.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if command == "/quit" or command == "/exit":
            self._save_and_exit()
            sys.exit(0)

        elif command == "/help":
            self.console.print(Panel(WELCOME, border_style="cyan", padding=(1, 2)))

        elif command == "/clear":
            self.session_manager.save_session(
                self.orchestrator.conversation_history, last_config=self.last_config
            )
            self.orchestrator.conversation_history.clear()
            self.last_config = None
            self.session_manager.new_session()
            self.console.print("  Cleared. New session started.", style="info")

        elif command == "/save":
            path = arg.strip() or "seatunnel_job.conf"
            self._save_config(path)

        elif command == "/run":
            self._run_config()

        elif command == "/check":
            self._check_config()

        elif command == "/connectors":
            from .connectors import list_connector_names
            names = list_connector_names()
            self.console.print("\n[heading]Available Connectors:[/heading]")
            self.console.print(f"  [bold]Sources:[/bold] {', '.join(names['sources'])}")
            self.console.print(f"  [bold]Sinks:[/bold]   {', '.join(names['sinks'])}")
            self.console.print(f"  [bold]Transforms:[/bold] {', '.join(names['transforms'])}")

        elif command == "/config":
            self._cmd_config(arg.strip())

        elif command == "/sessions":
            self._cmd_sessions()

        elif command == "/resume":
            self._cmd_resume(arg.strip())

        elif command == "/new":
            self._cmd_new_session()

        elif command == "/memory":
            self._cmd_memory()

        elif command == "/remember":
            self._cmd_remember(arg.strip())

        elif command == "/forget":
            self._cmd_forget(arg.strip())

        else:
            self.console.print(f"  Unknown command: {command}. Type /help for help.", style="warning")

    _CONFIG_HEADER = """\
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Generated by SeaTunnel CLI
#

"""

    def _auto_save_config(self) -> str | None:
        """Auto-save config to default location after generation. Returns saved path."""
        if not self.last_config:
            return None
        default_path = self.history_dir / "last_job.conf"
        try:
            with open(default_path, "w") as f:
                f.write(self._CONFIG_HEADER + self.last_config + "\n")
            return str(default_path)
        except Exception:
            return None

    def _save_config(self, path: str):
        """Save last generated config to a user-specified path."""
        if not self.last_config:
            self.console.print("  No config to save. Generate one first.", style="warning")
            return

        path = os.path.expanduser(path)
        try:
            with open(path, "w") as f:
                f.write(self._CONFIG_HEADER + self.last_config + "\n")
            self.console.print(f"  Config saved to: [bold]{path}[/bold]", style="success")
        except Exception as e:
            self.console.print(f"  Failed to save: {e}", style="error")

    def _check_config(self):
        """Dry-run validate last generated config, auto-fix on failure."""
        if not self.last_config:
            self.console.print("  No config to check. Generate one first.", style="warning")
            return

        from .agents import dry_run_config

        self.console.print("\n  Running dry-run validation...", style="info")
        result = dry_run_config(self.last_config)

        # Phase 1 result
        phase1 = result["phase1_local"]
        if phase1.startswith("VALID"):
            self.console.print("  [1] Local validation: [bold green]PASS[/bold green]", style="info")
        else:
            self.console.print("  [1] Local validation: [bold red]FAIL[/bold red]", style="error")
            self.console.print(f"      {phase1}", style="error")

        # Phase 2 result
        phase2 = result["phase2_check"]
        if phase2 is None:
            self.console.print(
                "  [2] Engine --check:   [bold yellow]SKIPPED[/bold yellow] (seatunnel.sh not found)",
                style="info",
            )
        elif phase2 == "PASS":
            self.console.print("  [2] Engine --check:   [bold green]PASS[/bold green]", style="info")
        else:
            self.console.print("  [2] Engine --check:   [bold red]FAIL[/bold red]", style="error")
            self.console.print(f"      {phase2}", style="error")

        # Overall
        self.console.print()
        if result["valid"]:
            self.console.print(
                Panel("Config is ready to execute.", title="Dry-run PASSED", border_style="green")
            )
        else:
            self.console.print(
                Panel(result["summary"], title="Dry-run Issues Found", border_style="red")
            )
            self._show_error_and_diagnose(result["summary"])

    def _run_config(self):
        """Execute config with SeaTunnel engine (REST API or CLI fallback).

        SECURITY: Always requires explicit user confirmation before execution.
        This prevents accidental writes to production databases.
        """
        if not self.last_config:
            self.console.print("  No config to run. Generate one first.", style="warning")
            return

        from .connectors import _check_engine, _ENGINE_API_BASE

        # ── Security: mandatory confirmation before execution ──
        self.console.print()
        self.console.print(
            Panel(
                "[bold yellow]⚠️  You are about to execute a SeaTunnel job.[/bold yellow]\n\n"
                "This will read from SOURCE systems and write to SINK systems.\n"
                "Make sure the config targets the correct environment (dev/staging/prod).",
                border_style="yellow",
                padding=(1, 2),
            )
        )

        # Show config summary: extract source/sink connector names
        config_preview = self.last_config[:1500]
        connectors = []
        for line in config_preview.split("\n"):
            stripped = line.strip()
            # Match connector block openings like "  Jdbc {" or "  S3File {"
            if stripped and stripped.endswith("{") and not stripped.startswith("#"):
                name = stripped.rstrip(" {").strip()
                if name and name not in ("env", "source", "sink", "transform") and not name.startswith("//"):
                    connectors.append(name)

        if _check_engine():
            target = f"REST API → {_ENGINE_API_BASE}"
        else:
            target = "Local CLI (seatunnel.sh -e local)"

        self.console.print(f"  Target:     [bold]{target}[/bold]")
        if connectors:
            self.console.print(f"  Connectors: [bold]{' → '.join(connectors)}[/bold]")

        try:
            confirm = pt_prompt(
                "\n  Type 'yes' to execute, anything else to cancel: ",
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            confirm = ""

        if confirm != "yes":
            self.console.print("  Execution cancelled.", style="info")
            return

        # Prefer REST API submission for better feedback
        if _check_engine():
            self._run_via_rest_api(_ENGINE_API_BASE)
            return

        # Fallback to CLI
        self._run_via_cli()

    def _run_via_rest_api(self, api_base: str):
        """Submit job via REST API and poll status."""
        import json as _json
        import urllib.request
        import urllib.error

        self.console.print("  Submitting job via REST API...", style="info")
        try:
            url = f"{api_base}/submit-job?format=hocon"
            req = urllib.request.Request(
                url,
                data=self.last_config.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = _json.loads(resp.read().decode("utf-8"))

            job_id = body.get("jobId")
            job_name = body.get("jobName", "")
            self.console.print(f"  Job submitted: [bold]{job_id}[/bold] ({job_name})", style="success")
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            self.console.print(f"  [error]Submit failed ({e.code}):[/error]")
            self._show_error_and_diagnose(error_body)
            return
        except Exception as e:
            self.console.print(f"  [error]Submit failed: {e}[/error]")
            return

        # Poll job status
        self._poll_job_status(api_base, str(job_id))

    def _poll_job_status(self, api_base: str, job_id: str):
        """Poll job status until terminal state, then show results."""
        import json as _json
        import urllib.request

        terminal_states = {"FINISHED", "CANCELED", "FAILED"}
        self.console.print("  Waiting for job to complete...", style="info")

        poll_interval = 2
        max_polls = 150  # 5 minutes max
        last_status = ""

        for _ in range(max_polls):
            time.sleep(poll_interval)
            try:
                url = f"{api_base}/job-info/{job_id}"
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    info = _json.loads(resp.read().decode("utf-8"))

                status = info.get("jobStatus", "UNKNOWN")
                if status != last_status:
                    last_status = status
                    self.console.print(f"  Status: [bold]{status}[/bold]", style="info")

                if status in terminal_states:
                    self._show_job_result(info)
                    return
            except Exception:
                pass

        self.console.print(
            f"  [warning]Polling timeout. Check job status manually: GET {api_base}/job-info/{job_id}[/warning]"
        )

    def _show_job_result(self, job_info: dict):
        """Display final job result and feed errors back to LLM context."""
        status = job_info.get("jobStatus", "UNKNOWN")
        error_msg = job_info.get("errorMsg", "")
        metrics = job_info.get("metrics", {})

        if status == "FINISHED":
            summary_parts = []
            for key in ("TableSourceReceivedCount", "TableSinkWriteCount"):
                if key in metrics:
                    summary_parts.append(f"{key}={metrics[key]}")
            metrics_text = ", ".join(summary_parts) if summary_parts else "no metrics"
            self.console.print(
                Panel(
                    f"Job completed successfully.\n{metrics_text}",
                    title="Job FINISHED",
                    border_style="green",
                )
            )
        elif status == "CANCELED":
            self.console.print(
                Panel("Job was canceled.", title="Job CANCELED", border_style="yellow")
            )
        elif status == "FAILED":
            self.console.print(
                Panel(
                    error_msg[:2000] if error_msg else "Unknown error",
                    title="Job FAILED",
                    border_style="red",
                )
            )
            if error_msg:
                self._show_error_and_diagnose(error_msg)

    def _show_error_and_diagnose(self, error_text: str):
        """Diagnose error and directly patch the existing config, with conversation memory."""
        from .memory import redact_credentials
        truncated = error_text[:3000]
        self.console.print("  Diagnosing and fixing config...", style="info")

        repair_system = (
            "You are a SeaTunnel config repair expert.\n"
            "The user's job failed. You have the full conversation history for context.\n"
            "Your job:\n"
            "1. Analyze the root cause (brief, 1-3 lines)\n"
            "2. Fix the config — patch it, do NOT rewrite from scratch\n"
            "3. Return the fixed config in a ```hocon block\n"
            "4. After the config block, explain what you changed in a ## Changes section\n\n"
            "CRITICAL RULES:\n"
            "- Keep ALL existing config values. Only add/change the minimum needed to fix the error.\n"
            "- NEVER put sink-only options (schema_save_mode, data_save_mode, generate_sink_sql) on a source.\n"
            "- NEVER put source-only options (partition_num, fetch_size) on a sink.\n"
            "- S3 credential keys use DASHES: fs.s3a.access-key (NOT fs.s3a.access.key)\n"
            "- NEVER hardcode passwords/secrets/keys — ALWAYS use ${ENV_VAR} placeholders for credentials.\n"
            "- If the error is about missing credentials, use ${ENV_VAR} placeholders and LIST them in the Changes section.\n"
        )
        # Security: replace credential values with ${PLACEHOLDER} before sending to LLM.
        # We keep a mapping so we can restore original values after LLM returns.
        safe_config, cred_map = _replace_creds_with_placeholders(
            self.last_config
        ) if self.last_config else ("", {})
        safe_error = redact_credentials(truncated)
        repair_msg = (
            f"Job execution failed with this error:\n\n```\n{safe_error}\n```\n\n"
            f"The config that failed:\n```hocon\n{safe_config}\n```\n\n"
            f"Fix this config. Only change what's needed to resolve the error."
        )

        history = self.orchestrator.conversation_history
        history.append({"role": "user", "content": [{"text": repair_msg}]})

        try:
            response = self.client.chat(
                messages=history,
                system=repair_system,
                temperature=0.1,
                max_tokens=4096,
            )
            reply_text = self.client.extract_text(response)

            history.append({
                "role": "assistant",
                "content": [{"text": reply_text}],
            })

            from .agents import Orchestrator
            parsed = Orchestrator._parse_config_response(reply_text)
            fixed_config = parsed.get("config")
            explanation = parsed.get("explanation", "")

            # Restore original credential values that LLM preserved as ${PLACEHOLDER}
            if fixed_config and cred_map:
                fixed_config = _restore_creds_from_placeholders(fixed_config, cred_map)

            if fixed_config and fixed_config != self.last_config:
                self.last_config = fixed_config
                self.console.print()
                self.console.print(
                    Panel(
                        Syntax(fixed_config, "properties", theme="monokai", line_numbers=True),
                        title="🔧 Fixed Config",
                        border_style="green",
                        padding=(1, 2),
                    )
                )
                if explanation:
                    self.console.print(
                        Panel(Markdown(explanation), title="Changes", border_style="cyan", padding=(1, 2))
                    )
                saved_path = self._auto_save_config()
                if saved_path:
                    self.console.print(f"  Config saved to: [bold]{saved_path}[/bold]", style="success")
                self.console.print(
                    "  Use [bold]/check[/bold] to validate, then [bold]/run[/bold] to retry.",
                    style="info",
                )
            else:
                self.console.print()
                self.console.print(
                    Panel(Markdown(reply_text), title="🔍 Diagnosis", border_style="yellow", padding=(1, 2))
                )
        except Exception as e:
            self.console.print(f"  [warning]Diagnosis failed: {e}[/warning]")

    def _run_via_cli(self):
        """Execute config via seatunnel.sh CLI with output capture."""
        import subprocess

        tmp_path = self.history_dir / "last_job.conf"
        with open(tmp_path, "w") as f:
            f.write(self.last_config + "\n")

        from .agents import _find_seatunnel_sh
        sh_path = _find_seatunnel_sh()

        if not sh_path:
            self.console.print(
                f"  [warning]SeaTunnel not found.[/warning]\n"
                f"  Set SEATUNNEL_HOME or start the engine for REST API mode.\n"
                f"  Manual run: [bold]sh bin/seatunnel.sh --config {tmp_path} -e local[/bold]"
            )
            return

        cmd = ["sh", sh_path, "--config", str(tmp_path), "-e", "local"]
        self.console.print(f"  Running: [bold]{' '.join(cmd)}[/bold]", style="info")

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if proc.stdout:
                stdout_tail = proc.stdout[-3000:]
                self.console.print(Panel(stdout_tail, title="Output", border_style="dim"))

            if proc.returncode == 0:
                self.console.print(
                    Panel("Job completed successfully.", title="Job FINISHED", border_style="green")
                )
            else:
                stderr_tail = proc.stderr[-3000:] if proc.stderr else "No error output"
                self.console.print(
                    Panel(stderr_tail, title=f"Job FAILED (exit code {proc.returncode})", border_style="red")
                )
                self._show_error_and_diagnose(stderr_tail)

        except subprocess.TimeoutExpired:
            self.console.print("  [warning]Job timed out (5 min). It may still be running.[/warning]")
        except Exception as e:
            self.console.print(f"  [error]Failed to execute: {e}[/error]")


def main():
    """Entry point for seatunnel CLI."""
    parser = argparse.ArgumentParser(
        prog="seatunnel",
        description="SeaTunnel CLI - Generate Apache SeaTunnel configs with natural language",
    )
    parser.add_argument(
        "-V", "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "request",
        nargs="?",
        help='Natural language request (e.g., "Sync MySQL users table to S3 Parquet")',
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path for generated config",
    )
    parser.add_argument(
        "--provider",
        choices=["bedrock", "anthropic", "openai"],
        help="LLM provider (overrides AI_PROVIDER env var and config.json)",
    )
    parser.add_argument(
        "--model",
        help="Override primary model ID",
    )
    parser.add_argument(
        "--fast-model",
        help="Override fast/small model ID",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Interactive first-time setup: choose LLM provider and save config",
    )
    parser.add_argument(
        "--export-metadata",
        nargs="?",
        const="auto",
        metavar="OUTPUT_PATH",
        help="Export connector metadata via Java runtime reflection (seatunnel-metadata-export.sh). "
             "Produces 100%% accurate connector_metadata.json. "
             "If OUTPUT_PATH is omitted, writes to the CLI .data/ directory.",
    )

    args = parser.parse_args()

    # --init: interactive setup, no LLM needed
    if args.init:
        console = Console(theme=THEME)
        SeaTunnelCLI.run_init(console)
        sys.exit(0)

    # --export-metadata: run Java exporter, no LLM needed
    if args.export_metadata:
        from .connectors import export_runtime_metadata
        output = None if args.export_metadata == "auto" else args.export_metadata
        result = export_runtime_metadata(output)
        if result:
            print(f"Metadata exported to: {result}")
            sys.exit(0)
        else:
            print("Error: Failed to export metadata. "
                  "Make sure SEATUNNEL_HOME is set and seatunnel-metadata-export.sh is available.",
                  file=sys.stderr)
            sys.exit(1)

    # Override provider if specified via CLI flags
    if args.provider:
        os.environ["AI_PROVIDER"] = args.provider
    if args.model:
        provider = os.environ.get("AI_PROVIDER", "").lower()
        if provider == "openai":
            os.environ["OPENAI_MODEL"] = args.model
        else:
            os.environ["ANTHROPIC_MODEL"] = args.model
    if args.fast_model:
        provider = os.environ.get("AI_PROVIDER", "").lower()
        if provider == "openai":
            os.environ["OPENAI_SMALL_FAST_MODEL"] = args.fast_model
        else:
            os.environ["ANTHROPIC_SMALL_FAST_MODEL"] = args.fast_model

    console = Console(theme=THEME)

    # Install credential-redacting log filter before any provider init
    _install_secret_log_filter()

    cli = SeaTunnelCLI(console)

    if args.request:
        cli.run_single(args.request, args.output)
    else:
        cli.run_interactive()


# ─── Credential-safe logging ───

# Patterns that indicate a value is a secret and must be redacted in logs.
_SECRET_PATTERNS = re.compile(
    r'(sk-ant-[A-Za-z0-9_-]+)'       # Anthropic API key
    r'|(sk-[A-Za-z0-9_-]{20,})'       # OpenAI API key
    r'|(AKIA[A-Z0-9]{16})'            # AWS Access Key ID
    r'|([A-Za-z0-9/+=]{40,})'         # AWS Secret Key / generic long token
    r'|(api[_-]?key\s*[:=]\s*\S+)'    # key=value patterns
    r'|(password\s*[:=]\s*\S+)'
    r'|(secret\s*[:=]\s*\S+)'
    r'|(token\s*[:=]\s*\S+)',
    re.IGNORECASE,
)


class _SecretRedactFilter(logging.Filter):
    """Logging filter that redacts credentials from log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg and isinstance(record.msg, str):
            record.msg = _SECRET_PATTERNS.sub("***REDACTED***", record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: _SECRET_PATTERNS.sub("***REDACTED***", str(v))
                    if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    _SECRET_PATTERNS.sub("***REDACTED***", str(a))
                    if isinstance(a, str) else a
                    for a in record.args
                )
        return True


def _install_secret_log_filter():
    """Install the secret-redacting filter on the root logger and all seatunnel_cli loggers."""
    secret_filter = _SecretRedactFilter()
    root_logger = logging.getLogger()
    root_logger.addFilter(secret_filter)
    for handler in root_logger.handlers:
        handler.addFilter(secret_filter)
    # Also cover our package loggers explicitly
    for name in ("seatunnel_cli", "seatunnel_cli.llm_provider",
                 "seatunnel_cli.agents", "seatunnel_cli.connectors",
                 "seatunnel_cli.memory"):
        pkg_logger = logging.getLogger(name)
        pkg_logger.addFilter(secret_filter)


if __name__ == "__main__":
    main()
