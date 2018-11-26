# Deployment

> The deployment of Waterdrop relies on the Java and Spark. The detail of Waterdrop installation steps refer to [Install Waterdrop](/en/installation).


The following parts focus on how operating Waterdrop on different platforms.

### Runing Waterdrop locally

```
./bin/start-waterdrop.sh --master local[4] --deploy-mode client --config ./config/application.conf
```

### Waterdrop on Spark Standalone Mode

```
# client mode
./bin/start-waterdrop.sh --master spark://207.184.161.138:7077 --deploy-mode client --config ./config/application.conf

# cluster mode
./bin/start-waterdrop.sh --master spark://207.184.161.138:7077 --deploy-mode cluster --config ./config/application.conf
```

### Waterdrop on Yarn

```
# client mode
./bin/start-waterdrop.sh --master yarn --deploy-mode client --config ./config/application.conf

# cluster mode
./bin/start-waterdrop.sh --master yarn --deploy-mode cluster --config ./config/application.conf
```

### Waterdrop on Mesos

```
# cluster mode
./bin/start-waterdrop.sh --master mesos://207.184.161.138:7077 --deploy-mode cluster --config ./config/application.conf
```

---


The `master` and `deploy-mode` parameter of start-waterdrop.sh has the same meaning as `master`, `deploy-mode` of spark-submit command line options.

Refer to: [Spark Submitting Applications](http://spark.apache.org/docs/latest/submitting-applications.html)

If you want to specify the size of the resource used by the Waterdrop application, or other Spark parameters, you can specify it in the configuration file:

```
spark {
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  ...
}
...

```

Detail of Waterdrop configuration, refer to [Waterdrop Configuration](/en/configuration/base).
