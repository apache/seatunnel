#  Base Configuration

## Core Concepts

- `Row` is a single line in the logical sense of `Waterdrop` and the basic unit of data processing. When processing data in `Filter`, all data will be treated as `Row`.
- `Field` is a field of `Row`. `Row` can contain fields with nested structure.
- `raw_message` refers to the raw_message field of input data in `Row`.
- `__root__` means top level of the `Row`. Commonly used to specify where new fields are generated in Row during data processing.

## Configuration

A complete Waterdrop configuration includes `spark`, `input`, `filter` and `output`. For example:

```
spark {
    ...
}

input {
    ...
}

filter {
    ...
}

output {
    ...
}
``` 

* `spark` is configuration of Spark

    For more of Spark parameters, please see [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html#available-properties). But the parameters of **master** and **deploy-mode** cannot be configured here, and you should specify it in Waterdrop startup script.

* `input` is used to configured input plugins, the configurations are differ in plugins.  
* `filter` is used to configured filter plugins, the configurations are differ in plugins.  

    The multiple plugins in the `filter` form a pipeline for data processing in the order of configuration, and the output of the previous filter is the input of the next filter.

* `output` is used to configured output plugins, the configurations are differ in plugins.

The data processed by the filter will be sent to each plugin configured in the output.

## Example Of Configuration

```
spark {
  # Waterdrop defined streaming batch duration in seconds
  spark.streaming.batchDuration = 5

  spark.app.name = "Waterdrop"
  spark.ui.port = 13000
}

input {
  socket {}
}

filter {
  split {
    fields = ["msg", "name"]
    delimiter = ","
  }
}

output {
  stdout {}
}
```