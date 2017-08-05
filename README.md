# Readme

## Requirements

Building Tool: SBT


## build and package

Build thin jar

```
# cd to project root dir

sbt package
```

Build fat jar(with all dependencies) for Spark Application

```
sbt clean assembly
```
