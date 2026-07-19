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

"""SeaTunnel CLI - Generate Apache SeaTunnel configs with natural language."""

import os
from pathlib import Path

__version__ = "0.1.0"


def get_seatunnel_home() -> str | None:
    """Return the SeaTunnel installation directory.

    Resolution order:
      1. SEATUNNEL_HOME env var (explicit override)
      2. Auto-detect from package location — in the distribution tarball the
         package lives at ``$SEATUNNEL_HOME/cli/seatunnel_cli/``, so going up
         two levels from ``__file__`` yields SEATUNNEL_HOME when
         ``bin/seatunnel.sh`` exists there.

    Returns the path string, or None if not found.
    """
    # 1. Explicit env var
    env = os.environ.get("SEATUNNEL_HOME", "").strip()
    if env and Path(env).is_dir():
        return env

    # 2. Auto-detect: __file__ = .../cli/seatunnel_cli/__init__.py
    #    -> parent.parent.parent = SEATUNNEL_HOME candidate
    candidate = Path(__file__).parent.parent.parent
    if (candidate / "bin" / "seatunnel.sh").exists():
        return str(candidate)

    return None


def get_data_dir() -> Path:
    """Return the CLI data directory (sessions, memory, config, cache).

    Resolution order:
      1. SEATUNNEL_CLI_DATA env var (explicit override)
      2. <seatunnel-cli project>/.data/  (co-located with the CLI package)

    All persistent files live here — never scattered to ~/.seatunnel/.
    """
    env_override = os.environ.get("SEATUNNEL_CLI_DATA")
    if env_override:
        p = Path(env_override)
    else:
        # seatunnel_cli/ -> seatunnel-cli/.data/
        p = Path(__file__).parent.parent / ".data"
    p.mkdir(parents=True, exist_ok=True)
    return p
