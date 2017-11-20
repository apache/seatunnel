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
# Linux, MACOS, Windows

sbt "-DprovidedDeps=true" clean assembly
```

---

## FAQs

1. Intellij Idea doesn't recognize antlr4 generated source ?

File -> Project Structure -> Modules, in `Sources` Tab, 
mark directory `target/scala-2.11/src_managed/main/antlr4/` as `Sources`(blue icon)


