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

import subprocess
import sys
import tempfile


def main():
    sys.stdin.readline()
    child_code = (
        "import time\n"
        "index = 0\n"
        "while index < 20:\n"
        "    print(f'{index + 2},python_child_{index}', flush=True)\n"
        "    index += 1\n"
        "    time.sleep(1)\n"
    )
    subprocess.Popen([sys.executable, "-c", child_code], cwd=tempfile.gettempdir())
    print("1,python_1", flush=True)


if __name__ == "__main__":
    main()
