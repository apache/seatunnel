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

import builtins
import json
import sys
import traceback


def _stderr_print(*args, **kwargs):
    kwargs.setdefault("file", sys.stderr)
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


def _write_response(payload):
    builtins.print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


def _load_script(script_path):
    namespace = {"print": _stderr_print}
    with open(script_path, "r", encoding="utf-8") as script_file:
        source = script_file.read()
    exec(compile(source, script_path, "exec"), namespace)
    process_fn = namespace.get("process")
    if not callable(process_fn):
        raise RuntimeError("Python transform script must define process(row, context)")
    return namespace, process_fn


def main():
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: worker_template.py <script_path>")

    namespace, process_fn = _load_script(sys.argv[1])
    open_fn = namespace.get("open")
    close_fn = namespace.get("close")

    init_line = sys.stdin.readline()
    if not init_line:
        raise RuntimeError("Python transform worker did not receive init payload")
    init_payload = json.loads(init_line)
    init_id = init_payload.get("id")
    context = init_payload.get("context", {})

    if callable(open_fn):
        open_fn(context)
    _write_response({"id": init_id, "ok": True})

    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            request = json.loads(line)
            request_id = request.get("id")
            try:
                result = process_fn(request.get("row"), context)
                _write_response({"id": request_id, "result": result})
            except Exception:
                _write_response({"id": request_id, "error": traceback.format_exc()})
    finally:
        if callable(close_fn):
            close_fn()


if __name__ == "__main__":
    main()
