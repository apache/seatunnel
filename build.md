# Readme

## Requirements

Building Tool: SBT

## Coding Style Check

```
sbt scalastyle
```

## build and package

Build thin jar

```
# cd to project root dir

sbt package
```

Build fat jar(with all dependencies) for Spark Application

```
sbt -DprovidedDeps=true clean assembly
```

Package Distribution

```
sbt -DprovidedDeps=true  universal:packageBin

# you can find distribution here:
target/universal/waterdrop-<version>.zip
```

If you want to check what files/directories will be included distribution package

```
sbt -DprovidedDeps=true stage
ls ./target/universal/stage/
```

check sbt native packager [universal plugin](http://www.scala-sbt.org/sbt-native-packager/formats/universal.html#universal-plugin) for more details.

---

## FAQs

1. Intellij Idea doesn't recognize antlr4 generated source ?

File -> Project Structure -> Modules, in `Sources` Tab, 
mark directory `target/scala-2.11/src_managed/main/antlr4/` as `Sources`(blue icon)

