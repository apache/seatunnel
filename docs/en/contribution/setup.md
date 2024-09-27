# Set Up Develop Environment

In this section, we are going to show you how to set up your development environment for SeaTunnel, and then run a simple
example in your JetBrains IntelliJ IDEA.

> You can develop or test SeaTunnel code in any development environment that you like, but here we use
> [JetBrains IDEA](https://www.jetbrains.com/idea/) as an example to teach you to step by step.

## Prepare

Before we start talking about how to set up the environment, we need to do some preparation work. Make sure you already
have installed the following software:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) installed.
* [Java](https://www.java.com/en/download/) ( JDK8/JDK11 are supported by now) installed and `JAVA_HOME` set.
* [Scala](https://www.scala-lang.org/download/2.11.12.html) (only scala 2.11.12 supported by now) installed.
* [JetBrains IDEA](https://www.jetbrains.com/idea/) installed.

## Set Up

### Clone the Source Code

First of all, you need to clone the SeaTunnel source code from [GitHub](https://github.com/apache/seatunnel).

```shell
git clone git@github.com:apache/seatunnel.git
```

### Install Subproject Locally

After cloning the source code, you should run the `./mvnw` command to install the subproject to the maven local repository.
Otherwise, your code could not start in JetBrains IntelliJ IDEA correctly.

```shell
./mvnw install -Dmaven.test.skip
```

### Building SeaTunnel From Source

After you install the maven, you can use the following command to compile and package.

```
mvn clean package -pl seatunnel-dist -am -Dmaven.test.skip=true
```

### Building Sub Module

If you want to build submodules separately, you can use the following command to compile and package.

```ssh
# This is an example of building the redis connector separately

 mvn clean package -pl seatunnel-connectors-v2/connector-redis -am -DskipTests -T 1C
```

### Install JetBrains IDEA Scala Plugin

Now, you can open your JetBrains IntelliJ IDEA and explore the source code. But before building Scala code in IDEA,
you should also install JetBrains IntelliJ IDEA's [Scala Plugin](https://plugins.jetbrains.com/plugin/1347-scala).
See [Install Plugins For IDEA](https://www.jetbrains.com/help/idea/managing-plugins.html#install-plugins) if you want to.

### Install JetBrains IDEA Lombok Plugin

Before running the following example, you should also install JetBrains IntelliJ IDEA's [Lombok plugin](https://plugins.jetbrains.com/plugin/6317-lombok).
See [install plugins for IDEA](https://www.jetbrains.com/help/idea/managing-plugins.html#install-plugins) if you want to.

### Code Style

Apache SeaTunnel uses `Spotless` for code style and format checks. You can run the following command and `Spotless` will automatically fix the code style and formatting errors for you:

```shell
./mvnw spotless:apply
```

You could copy the `pre-commit hook` file `/tools/spotless_check/pre-commit.sh` to your `.git/hooks/` directory so that every time you commit your code with `git commit`, `Spotless` will automatically fix things for you.

## Run Simple Example

After all the above things are done, you just finish the environment setup and can run an example we provide to you out
of box. All examples are in module `seatunnel-examples`, you could pick one you are interested in, [Running Or Debugging
It In IDEA](https://www.jetbrains.com/help/idea/run-debug-configuration.html) as you wish.

Here we use `seatunnel-examples/seatunnel-engine-examples/src/main/java/org/apache/seatunnel/example/engine/SeaTunnelEngineExample.java`
as an example, when you run it successfully you can see the output as below:

```log
2024-08-10 11:45:32,839 INFO  org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand - 
***********************************************
           Job Statistic Information
***********************************************
Start Time                : 2024-08-10 11:45:30
End Time                  : 2024-08-10 11:45:32
Total Time(s)             :                   2
Total Read Count          :                   5
Total Write Count         :                   5
Total Failed Count        :                   0
***********************************************
```

## What's More

All our examples use simple source and sink to make it less dependent and easy to run. You can change the example configuration
in `resources/examples`. You can change your configuration as below, if you want to use PostgreSQL as the source and
sink to console.
Please note that when using connectors other than FakeSource and Console, you need to modify the dependencies in the `pom.xml` file of the corresponding submodule of seatunnel-example.

```conf
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
    Jdbc {
        driver = org.postgresql.Driver
        url = "jdbc:postgresql://host:port/database"
        username = postgres
        password = "123456"
        query = "select * from test"
        table_path = "database.test"
    }
}

sink {
  Console {}
}
```

