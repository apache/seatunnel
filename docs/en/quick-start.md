# Quick Start


> We will show how to use Waterdrop by taking an application that receives data from a socket, splits the data into multiple fields, and outputs the processed result.

### Step 1: Prepare Spark Runtime

> If you are familiar with Spark or you have prepared the Spark runtime environment, you can ignore this step. You do not need do any special configuration in Waterdrop.

Please [download Spark](http://spark.apache.org/downloads.html) firstly. Require Spark version 2.0.0 or later. After download and extract, you can submit Spark application on client mode without any Spark configuration. If you want to launching applications on a cluster, please refer to the Spark configuration document.


### Step 2: Download Waterdrop

Download [Waterdrop](https://github.com/InterestingLab/waterdrop/releases) and extract it.

```
# Take waterdrop 1.0.2 as example:
wget https://github.com/InterestingLab/waterdrop/releases/download/v1.0.2/waterdrop-1.0.2.zip -O waterdrop-1.0.2.zip
unzip waterdrop-1.0.2.zip
ln -s waterdrop-1.0.2 waterdrop
```

### Step 3: Waterdrop Configuration

Edit `config/waterdrop-env.sh`, specify the environment configuration must by used such as SPARK_HOME (the directory of Spark in Step 1)

Edit `config/application.conf`, which determined your Waterdrop pipeline, included input, filter and output.

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

You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using
```
nc -l -p 9999
```


### Step 5: Running Waterdrop

```
cd waterdrop
./bin/start-waterdrop.sh --master local[4] --deploy-mode client --config ./config/application.conf

```

### Step 6: Typing your lines

```
Hello World, Gary
```

The output of Waterdrop like this:

```
+-----------------+-----------+----+
|raw_message      |msg        |name|
+-----------------+-----------+----+
|Hello World, Gary|Hello World|Gary|
+-----------------+-----------+----+
```


### Conclusion

Waterdrop is easy to use and there are richer data processing capability waiting to be discovered. The data processing case presented in this article does not require any code, compilation or packing. It is simpler than Spark [Quick Example](https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example).