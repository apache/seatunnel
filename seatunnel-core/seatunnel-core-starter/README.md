# Introduction

This module is the base start module for SeaTunnel new connector API.

![seatunnel_architecture.png](../../docs/en/images/seatunnel_architecture.png)

# SeaTunnel Job Execute Process

The first step, SeaTunnel runtime engine will get job definition from seatunnel.conf file, then parse the config, load
seatunnel plugin from classpath/FileSystem. After initialize seatunnel plugin, SeaTunnel runtime engine will translate
the job to target engine(Flink/Spark) job, then submit the job to target engine.

