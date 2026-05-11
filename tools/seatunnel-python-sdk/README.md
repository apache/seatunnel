## Installation

The SDK is not published to PyPI yet. Install it directly from the SeaTunnel source tree:

```bash
git clone https://github.com/apache/seatunnel.git
cd seatunnel
python -m pip install ./tools/seatunnel-python-sdk
```

Python 3.9 or newer is required.

## Usage

With a server already running at port 8080, this example submits a job:

``` py
from seatunnel import SeaTunnelClient, SubmitJobQueryParams

config = """
env {
  job.mode = "batch"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 1000
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

transform {
}

sink {
  Console {
    plugin_input = "fake"
  }
}
"""

with SeaTunnelClient(base_url="http://localhost:8080") as client:
  query_params = SubmitJobQueryParams()
  response = client.jobs.submit_job(conf=config, params=query_params)
  print(response)
```
