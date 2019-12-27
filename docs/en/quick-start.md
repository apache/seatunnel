# Quick Start

> This is a getting-started guide, it ingests data from socket then splits data into multiple fields, 
> and finally outputs the processed result.

### Step 1: Prepare Spark Runtime

> If you are familiar with Spark or you have prepared the Spark runtime environment, you can ignore this step. You do not need do any special configuration in Spark.

Please [download Spark](http://spark.apache.org/downloads.html) firstly. Spark version 2.0.0 or later is required. After downloading and extracting, 
you can submit Spark application on client mode without any Spark configuration. 
If you want to launch applications on a cluster, please refer to the official documents of [Deploying Spark](http://spark.apache.org/docs/latest/cluster-overview.html).


### Step 2: Download Waterdrop

Download [Waterdrop](https://github.com/InterestingLab/waterdrop/releases) and extract it.

```
# Take waterdrop 1.1.0 as an example:

wget https://github.com/InterestingLab/waterdrop/releases/download/v1.1.0/waterdrop-1.1.0.zip -O waterdrop-1.1.0.zip
unzip waterdrop-1.1.0.zip
ln -s waterdrop-1.1.0 waterdrop

```

### Step 3: Waterdrop Configuration

Edit `config/waterdrop-env.sh`, specify the necessary environment configuration, 
such as `SPARK_HOME` (the path of Spark folder in Step 1)

Edit `config/application.conf`, which determines how to build processing pipeline, 
including receiving, transforming and output data.

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

### Step 4: Running Netcat

You will first need to run Netcat (a small utility found in most Unix-like systems) 
as a data source

```
# start netcat in port 9999, waiting for input.
nc -l -p 9999
```


### Step 5: Running Waterdrop

```
cd waterdrop
./bin/start-waterdrop.sh --master local[4] --deploy-mode client --config ./config/application.conf

```

### Step 6: Typing your lines in Netcat

```
Hello World, Gary
```

The output of Waterdrop is: 

```
+-----------------+-----------+----+
|raw_message      |msg        |name|
+-----------------+-----------+----+
|Hello World, Gary|Hello World|Gary|
+-----------------+-----------+----+
```


### Conclusion

Waterdrop is easy to use and there are rich data processing capabilities waiting to be discovered.
The data processing example presented in this page does not require any code work, 
compilation or packaging. 
It is much more simpler than Spark [Quick Example](https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example).
