# Introduction of plugins directory
This directory used to store some plugin configuration files. 

- `json/files/schemas/` is the default schema store directory for [Json transform plugin](https://seatunnel.apache.org/docs/transform/json#schema_dir-string).

If you use spark cluster mode, this directory will be sent to the executor by `--files`.