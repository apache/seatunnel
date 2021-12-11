# SeaTunnel Docker Images

## Quick Start Image

### Introduction

For quick start usage, packaged with Apache Spark and Apache Flink releases, only run in standalone mode.

the spark home is in `/opt/spark`, the flink home is in `/opt/flink`, the seatunnel home is in `/opt/seatunnel`.

### Build Image

Change directory to project root, then run maven package command with `docker-quick-start` profile enabled(disabled by default):

```shell
mvn clean package -Pdocker-quick-start
```

you can choose whether to package spark or flink within the image:

```shell
mvn clean package -Pdocker-quick-start\
 -Dinstall.spark=true\
 -Dinstall.flink=false\
 -Dimage.name='seatunnel:${project.version}'
```

you can specify the mirror url to speed up the download process:

```shell
mvn clean package -Pdocker-quick-start\
 -Dspark.archive.mirror=https://archive.apache.org/dist/spark/spark-2.4.0\
 -Dspark.archive.name=spark-2.4.0-bin-hadoop2.7.tgz\
 -Dflink.archive.mirror=https://archive.apache.org/dist/flink/flink-1.9.0\
 -Dflink.archive.name=flink-1.9.0-bin-scala_2.11.tgz
```

you'll find the image tagged with name `seatunnel:<version>` if success.

### Run Container

Run command:

```shell
docker run \
  -it \
  --name seatunnel-demo \
  --net host \
  -v <host_job_conf_dir>:<container_job_conf_dir> \
  seatunnel[:<version>] \
  [params...]
```

parameters:

| Option   | Candidates   | Explain                                             |
| -------- | ------------ | --------------------------------------------------- |
| --engine | spark, flink | specify which engine to run on                      |
| --config | *            | path to the job config file within docker container |

you can specify `--engine=spark` to run on Spark engine, or specify `--engine=flink` to run on Flink engine.

if you run on spark engine, you can find spark master's UI on http://localhost:8080.

if you run on flink engine, you can find flink UI on http://localhost:8081.

the default job config file for spark engine is `config/spark.batch.conf.template`, and for flink is `config/flink.streaming.conf.template`.

other parameters will be passed to corresponding bootstrap script: `start-seatunnel-spark.sh` or `start-seatunnel-flink.sh`.

