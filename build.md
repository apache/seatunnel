# Readme

## Requirements

Building Tool: SBT

## Coding Style Check

```
sbt scalastyle
```
## Show Dependency Tree

```
sbt dependencyTree > tree.log
```

## build and package

Build thin jar

```
# cd to project root dir

sbt package
```

Build fat jar(with all dependencies) for Spark Application

```
# on Linux, MACOS
sbt "-DprovidedDeps=true" clean assembly

# on Windows
set JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8'
sbt package
```

Package Distribution

```
# on Linux, Mac
sbt "-DprovidedDeps=true"  universal:packageBin

# on Windows
set JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8'
sbt "-DprovidedDeps=true"  universal:packageBin

# When packaging finished, you can find distribution here:
target/universal/waterdrop-<version>.zip
```

If you want to check what files/directories will be included distribution package

```
sbt "-DprovidedDeps=true" stage
ls ./target/universal/stage/
```

check sbt native packager [universal plugin](http://www.scala-sbt.org/sbt-native-packager/formats/universal.html#universal-plugin) for more details.

---

## FAQs

1.  Intellij IDEA doesn't recognize antlr4 generated source ?

File -> Project Structure -> Modules, in `Sources` Tab, 

select module `waterdrop-core`, then
mark directory `target/scala-2.11/src_managed/main/antlr4/` as `Sources Root`(blue icon)

> If you don't have `target/scala-2.11/src_managed/main/antlr4/` directory, you can create this directory,
> or execute `sbt package` in command line first.

2.  OutOfMemoryError occurs while compilation ?

```
# Linux, Mac
export JAVA_OPTS=-Xmx4G
sbt ...

# Windows
set JAVA_OPTS=-Xmx4G
sbt ...
```

3.  How to auto-format code according to the coding style of this project in Intellij IDEA ？

The coding style is defined in `scalastyle-config.xml` in this project root dir. 
When you are using Intellij IDEA, you can install scalafmt plugin to auto-format code style.
Then you should check "Format on file save" in scalafmt configuration(in preferences or settings).
