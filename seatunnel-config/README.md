# Introduction
The `seatunnel-config` is used to parse `seatunnel.conf` files. This module is based on `com.typesafe.config`, 
We have made some enhancement and import our enhancement by using maven shade. Most of the times, you don't need to directly 
using this module, since you can receive from maven repository.

# How to modify the config module
If you want to modify the config module, you can follow the steps below.
1. Open the `seatunnel-config` module.
```xml
<!--
    We retrieve the config module from maven repository. If you want to change the config module,
    you need to open this annotation and change the dependency of config-shade to project.
    <module>seatunnel-config</module>
-->
```
Open the annuotaion in `pom.xml` file, to import the `seatunnel-config` module.
2. Replace the `config-shade` dependency to project.
```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-config-shade</artifactId>
    <version>${project.version}</version>
</dependency>
```
Add `<version>${project.version}</version>` to `seatunnel-config-shade` everywhere you use.