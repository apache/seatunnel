## Local Installation

- Install dependencies:
`pip install -r requirements.txt`

- Build the package
`python setup.py sdist bdist_wheel`

- Install from the local package
`pip install ./dist/seatunnel-0.1-py3-none-any.whl --force-reinstall`

## Usage

With a server already running at port 8080, this example submits a job:

``` py
from seatunnel import SeaTunnelClient, SubmitJobQueryParams

client = SeaTunnelClient(base_url="http://localhost:8080")

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

query_params = SubmitJobQueryParams()
response = client.jobs.submit_job(conf=config, params=query_params)
print(response)
```