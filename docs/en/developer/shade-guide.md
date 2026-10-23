---
sidebar_position: 7
---

# SeaTunnel Shade Guide

## Overview

[Apache SeaTunnel Shade](https://github.com/apache/seatunnel-shade) is a separate repository that provides **shaded (package-relocated)** JARs of third-party libraries used by Apache SeaTunnel.

Each module wraps a single third-party library, relocating its packages under `org.apache.seatunnel.shade.*` to avoid classpath conflicts when SeaTunnel itself or its connectors depend on different versions of the same libraries.

For example, `seatunnel-shade-guava` shades Guava `27.0-jre` into `org.apache.seatunnel.shade.com.google.common.*`, so SeaTunnel can use a different Guava version internally without conflict.

## Available Modules

| Module | Library | Version |
|--------|---------|---------|
| `seatunnel-shade-guava` | Guava | 27.0-jre |
| `seatunnel-shade-jackson` | Jackson | 2.15.4 |
| `seatunnel-shade-commons-lang3` | Commons Lang3 | 3.18.0 |
| `seatunnel-shade-arrow` | Apache Arrow | 15.0.1 |
| `seatunnel-shade-hikari` | HikariCP | 4.0.3 |
| `seatunnel-shade-hazelcast` | Hazelcast | 5.1 |
| `seatunnel-shade-hadoop3-uber` | Hadoop Client | 3.1.4 |
| `seatunnel-shade-hadoop-aws` | Hadoop AWS | 3.1.4 |
| `seatunnel-shade-scala-compiler` | Scala Compiler | 2.12.15 |
| `seatunnel-shade-janino` | Janino | 3.1.12 |
| `seatunnel-shade-calcite` | Apache Calcite | 1.38.0 |
| `seatunnel-shade-jetty` | Jetty | 9.4.56 |
| `seatunnel-shade-thrift-service` | Apache Doris Thrift | 1.0.0 |

## Version Convention

Shade module versions follow the pattern:

```
${library.version}-${seatunnel.shade.version}
```

For example: `seatunnel-shade-guava-27.0-jre-3.0.0.jar`

- `27.0-jre` is the version of the shaded library (Guava)
- `3.0.0` is the SeaTunnel shade version (controlled by the `seatunnel.shade.version` property)

## Repository

- **GitHub**: [https://github.com/apache/seatunnel-shade](https://github.com/apache/seatunnel-shade)
- **Artifacts**: Published to [Maven Central](https://search.maven.org/search?q=org.apache.seatunnel) under `org.apache.seatunnel`

## How SeaTunnel Consumes Shade Modules

In SeaTunnel's root `pom.xml`, all shade modules are centrally managed in `<dependencyManagement>`:

```xml
<properties>
    <seatunnel.shade.version>3.0.0</seatunnel.shade.version>
    <seatunnel.shade.guava.version>27.0-jre</seatunnel.shade.guava.version>
    <seatunnel.shade.hadoop.version>3.1.4</seatunnel.shade.hadoop.version>
    <!-- ... other shade module versions ... -->
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>seatunnel-shade-guava</artifactId>
            <version>${seatunnel.shade.guava.version}-${seatunnel.shade.version}</version>
        </dependency>
        <!-- ... other shade modules ... -->
    </dependencies>
</dependencyManagement>
```

Child modules declare dependencies **without** a `<version>` element — the version is inherited from the root `dependencyManagement`:

```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-shade-guava</artifactId>
</dependency>
```

## Building the Shade Project

### Prerequisites

- Java 11+
- Maven 3.x

### Clone and Build

```bash
git clone https://github.com/apache/seatunnel-shade.git
cd seatunnel-shade

# Full build (skip tests, skip RAT license check)
mvn -B -DskipTests -Drat.skip=true clean install

# Build with parallelism (2x cores)
mvn -B -DskipTests -Drat.skip=true -T 2C clean install

# Build a single module with dependencies
mvn -B -DskipTests -pl seatunnel-shade-guava -am clean install
```

## Release Process

### When to Release

A shade module release is needed when:

- A third-party library version is upgraded (e.g., Guava 27.0 → 33.0)
- A new shade module is added
- The `seatunnel.shade.version` is bumped (affects **all** modules)

### Full Release (All Modules)

Deploy all modules to the Apache Maven repository:

```bash
mvn -B -DskipTests -Drat.skip=true clean deploy
```

### Partial Release (Selected Modules)

When changes only affect a subset of modules, use `-pl` to deploy only the changed ones. **Never run `mvn clean deploy` without `-pl`** — unchanged modules would be redeployed and Nexus will reject them with a 400 error.

```bash
# 1. Verify locally (skip GPG signing)
mvn -B -DskipTests -Drat.skip=true -Dgpg.skip=true \
  -pl seatunnel-shade-calcite,seatunnel-shade-janino,seatunnel-shade-scala-compiler \
  clean install

# 2. Deploy to Apache repository
mvn -B -DskipTests -Drat.skip=true \
  -pl seatunnel-shade-calcite,seatunnel-shade-janino,seatunnel-shade-scala-compiler \
  clean deploy
```

:::warning

Module versions follow `${library.version}-${seatunnel.shade.version}`. As long as the library version changes, there is no conflict with published artifacts. If only the shade plugin configuration changed (not the library version), you must bump the version manually before deploying.

:::

### Tagging

After a successful release, tag the commit:

```bash
git tag v3.0.0.fix
git push origin v3.0.0.fix
```

## Adding a New Shade Module

1. Add the new module to `<modules>` in the shade project's root `pom.xml`.

2. Add the version property:
   ```xml
   <seatunnel.shade.newlib.version>X.Y.Z</seatunnel.shade.newlib.version>
   ```

3. Create the module POM following the template:

   ```xml
   <project xmlns="http://maven.apache.org/POM/4.0.0" ...>
       <modelVersion>4.0.0</modelVersion>
       <parent>
           <groupId>org.apache.seatunnel</groupId>
           <artifactId>seatunnel-shade</artifactId>
           <version>3.0.0</version>
       </parent>

       <artifactId>seatunnel-shade-newlib</artifactId>
       <version>${newlib.version}-${seatunnel.shade.version}</version>

       <dependencies>
           <dependency>
               <groupId>com.example</groupId>
               <artifactId>newlib</artifactId>
               <version>${newlib.version}</version>
           </dependency>
       </dependencies>

       <build>
           <plugins>
               <plugin>
                   <groupId>org.apache.maven.plugins</groupId>
                   <artifactId>maven-shade-plugin</artifactId>
                   <executions>
                       <execution>
                           <goals><goal>shade</goal></goals>
                           <phase>package</phase>
                           <configuration>
                               <relocations>
                                   <relocation>
                                       <pattern>com.example</pattern>
                                       <shadedPattern>${seatunnel.shade.package}.com.example</shadedPattern>
                                   </relocation>
                               </relocations>
                           </configuration>
                       </execution>
                   </executions>
               </plugin>
           </plugin>
       </build>
   </project>
   ```

4. Publish the new module.

5. In the SeaTunnel project:
   - Add version property to root `pom.xml`
   - Add dependency entry in root `<dependencyManagement>`
   - Add the dependency to the connector or module that needs it

## Updating SeaTunnel After a Shade Release

After publishing new shade artifacts, update the SeaTunnel project:

| Step | Location |
|------|----------|
| Bump version property | Root `pom.xml` → `<properties>` → `seatunnel.shade.<lib>.version` |
| Declare dependency (if new) | Root `pom.xml` → `<dependencyManagement>` |
| Update code imports | All `.java` files using the library |

If only `seatunnel.shade.version` changes (e.g., `3.0.0` → `3.0.1`), it affects **all** shade dependencies and every module must be republished.

### Propagation Delay Between Shade Release and SeaTunnel CI

Once a new `seatunnel-shade-*` artifact is published to [Apache's release repository](https://repository.apache.org/content/repositories/releases/), Maven Central's global CDN typically needs **up to ~24 hours** to pick it up and serve it to all mirrors. During this window, the SeaTunnel main project — which resolves dependencies through Maven Central — will not be able to download the freshly released shade artifact, and CI will fail with `Could not find artifact org.apache.seatunnel:seatunnel-shade-*:...`.

If a release is bumped in the SeaTunnel root `pom.xml` immediately after the shade release, expect CI to be red for roughly the first day until Central propagation completes. If you need CI green right away, wait for Central to catch up before bumping the version in SeaTunnel (or rerun CI on the failing commit the next day — no code change needed).

You can confirm an artifact is live on Central by checking https://search.maven.org/search?q=org.apache.seatunnel or by querying:

```bash
curl -sI https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-shade-guava/<lib.version>-<shade.version>/seatunnel-shade-guava-<lib.version>-<shade.version>.pom
```

A `200 OK` means Central has propagated the release.
