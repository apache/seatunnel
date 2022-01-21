# Quick Start

## Start Flink

```shell
docker-compose up -d
```

Wait for a few minutes and check the healthiness of the job manager and task manager with `docker-compose ps`. When the
components are healthy, open your browser and navigate to http://localhost:8081, you can see the Flink page.

## Start netcat as data source

```shell
docker run -it --rm --network flink_local --name datasource alpine:3 nc -lk -p 9999
```

## Submit SeaTunnel jobs

```shell
docker run -it --rm \
  -v $(pwd):/app \
  -v $(pwd)/config/flink-conf.yaml:/flink/conf/flink-conf.yaml \
  --network flink_local \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
  apache/seatunnel-flink --config /app/config/application.conf
```

You will find the running job in `http://localhost:8081`.

## Test data

Type some data in the `datasource` container of [the step](#start-netcat-as-data-source).

## View the task manager logs

```shell
docker-compose logs -f taskmanager
```
