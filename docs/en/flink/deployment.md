# Deployment and run

`seatunnel For Flink` relies on the `Java` runtime environment and `Flink` . For detailed `seatunnel` installation steps, refer to [installing seatunnel](./installation.md)

The following focuses on how different platforms run:

> First edit the `config/seatunnel-env.sh` in the `seatunnel` directory after decompression, and specify the required environment configuration `FLINK_HOME`

## Run seatunnel on Flink Standalone cluster

```bash
bin/start-seatunnel-flink.sh \
--config config-path

# -p 2 specifies that the parallelism of flink job is 2. You can also specify more parameters, use flink run -h to view
bin/start-seatunnel-flink.sh \
-p 2 \
--config config-path
```

## Run seatunnel on Yarn cluster

```bash
bin/start-seatunnel-flink.sh \
-m yarn-cluster \
--config config-path

# -ynm seatunnel specifies the name displayed in the yarn webUI as seatunnel, you can also specify more parameters, use flink run -h to view
bin/start-seatunnel-flink.sh \
-m yarn-cluster \
-ynm seatunnel \
--config config-path
```

Refer to: [Flink Yarn Setup](https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/deployment/resource-providers/yarn)
