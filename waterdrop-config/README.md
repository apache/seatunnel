This project is a fork of https://github.com/lightbend/config, its mainly purpose is to solve these problems : 

* [Keep config item order in ConfigObject](https://github.com/lightbend/config/issues/365)

* Change path token separator from `"."` to `"->""`.

* Added `parseObjectForWaterdrop()` to parse config objects in `input`, `filter`, `output` for waterdrop.

* Change package name to avoid implementation conflict when using this code with the official typesafe config used by Apache Flink.


If you want to do some test, please see:

* Test Main Class: config/src/test/java/interestinglab/CompleteTests.java

* Config File Example: config/src/test/resources/interestinglab/variables.conf

If you want to Package the jar:

```
git checkout garyelephant.fea.changed_package_name
rm -rf ./target
rm -rf ./config/target
sbt package
```

then you can find `config-1.3.3.jar` in `./config/target/`

---

Configuration library for JVM languages.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.typesafe/config/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.typesafe/config)
[![Build Status](https://travis-ci.org/lightbend/config.svg?branch=master)](https://travis-ci.org/lightbend/config)

If you have questions or are working on a pull request or just
curious, please feel welcome to join the chat room:
[![Join chat https://gitter.im/lightbend/config](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/lightbend/config?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Overview

 - implemented in plain Java with no dependencies
 - supports files in three formats: Java properties, JSON, and a
   human-friendly JSON superset
 - merges multiple files across all formats
 - can load from files, URLs, or classpath
 - good support for "nesting" (treat any subtree of the config the
   same as the whole config)
 - users can override the config with Java system properties,
    `java -Dmyapp.foo.bar=10`
 - supports configuring an app, with its framework and libraries,
   all from a single file such as `application.conf`
 - parses duration and size settings, "512k" or "10 seconds"
 - converts types, so if you ask for a boolean and the value
   is the string "yes", or you ask for a float and the value is
   an int, it will figure it out.
 - JSON superset features:
    - comments
    - includes
    - substitutions (`"foo" : ${bar}`, `"foo" : Hello ${who}`)
    - properties-like notation (`a.b=c`)
    - less noisy, more lenient syntax
    - substitute environment variables (`logdir=${HOME}/logs`)
 - API based on immutable `Config` instances, for thread safety
   and easy reasoning about config transformations
 - extensive test coverage

This library limits itself to config files. If you want to load
config from a database or something, you would need to write some
custom code. The library has nice support for merging
configurations so if you build one from a custom source it's easy
to merge it in.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Essential Information](#essential-information)
  - [License](#license)
  - [Binary Releases](#binary-releases)
  - [Release Notes](#release-notes)
  - [API docs](#api-docs)
  - [Bugs and Patches](#bugs-and-patches)
  - [Build](#build)
- [Using the Library](#using-the-library)
  - [API Example](#api-example)
  - [Longer Examples](#longer-examples)
  - [Immutability](#immutability)
  - [Schemas and Validation](#schemas-and-validation)
  - [Standard behavior](#standard-behavior)
    - [Note about resolving substitutions in `reference.conf` and `application.conf`](#note-about-resolving-substitutions-in-referenceconf-and-applicationconf)
  - [Merging config trees](#merging-config-trees)
  - [How to handle defaults](#how-to-handle-defaults)
  - [Understanding `Config` and `ConfigObject`](#understanding-config-and-configobject)
  - [ConfigBeanFactory](#configbeanfactory)
- [Using HOCON, the JSON Superset](#using-hocon-the-json-superset)
  - [Features of HOCON](#features-of-hocon)
  - [Examples of HOCON](#examples-of-hocon)
  - [Uses of Substitutions](#uses-of-substitutions)
    - [Factor out common values](#factor-out-common-values)
    - [Inheritance](#inheritance)
    - [Optional system or env variable overrides](#optional-system-or-env-variable-overrides)
  - [Concatenation](#concatenation)
  - [`reference.conf` can't refer to `application.conf`](#referenceconf-cant-refer-to-applicationconf)
- [Miscellaneous Notes](#miscellaneous-notes)
  - [Debugging Your Configuration](#debugging-your-configuration)
  - [Supports Java 8 and Later](#supports-java-8-and-later)
  - [Rationale for Supported File Formats](#rationale-for-supported-file-formats)
  - [Other APIs (Wrappers, Ports and Utilities)](#other-apis-wrappers-ports-and-utilities)
    - [Guice integration](#guice-integration)
    - [Java (yep!) wrappers for the Java library](#java-yep-wrappers-for-the-java-library)
    - [Scala wrappers for the Java library](#scala-wrappers-for-the-java-library)
    - [Clojure wrappers for the Java library](#clojure-wrappers-for-the-java-library)
    - [Kotlin wrappers for the Java library](#kotlin-wrappers-for-the-java-library)
    - [Scala port](#scala-port)
    - [Ruby port](#ruby-port)
    - [Puppet module](#puppet-module)
    - [Python port](#python-port)
    - [C++ port](#c-port)
    - [JavaScript port](#javascript-port)
    - [C# port](#c-port-1)
    - [Linting tool](#linting-tool)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Essential Information

### Binary Releases

Version 1.2.1 and earlier were built for Java 6, while newer
versions (1.3.0 and above) will be built for Java 8.

You can find published releases on Maven Central.

    <dependency>
        <groupId>com.typesafe</groupId>
        <artifactId>config</artifactId>
        <version>1.3.2</version>
    </dependency>

sbt dependency:

    libraryDependencies += "com.typesafe" % "config" % "1.3.2"

Link for direct download if you don't use a dependency manager:

 - http://central.maven.org/maven2/com/typesafe/config/

### Release Notes

Please see NEWS.md in this directory,
https://github.com/lightbend/config/blob/master/NEWS.md

### API docs

 - Online: https://lightbend.github.io/config/latest/api/
 - also published in jar form
 - consider reading this README first for an intro
 - for questions about the `.conf` file format, read
   [HOCON.md](https://github.com/lightbend/config/blob/master/HOCON.md)
   in this directory

### Bugs and Patches

Report bugs to the GitHub issue tracker. Send patches as pull
requests on GitHub.

Before we can accept pull requests, you will need to agree to the
Typesafe Contributor License Agreement online, using your GitHub
account - it takes 30 seconds.  You can do this at
https://www.lightbend.com/contribute/cla

Please see
[CONTRIBUTING](https://github.com/lightbend/config/blob/master/CONTRIBUTING.md)
for more including how to make a release.

### Build

The build uses sbt and the tests are written in Scala; however,
the library itself is plain Java and the published jar has no
Scala dependency.

## Using the Library

### API Example
    import com.typesafe.config.ConfigFactory

    Config conf = ConfigFactory.load();
    int bar1 = conf.getInt("foo.bar");
    Config foo = conf.getConfig("foo");
    int bar2 = foo.getInt("bar");

### Longer Examples

See the examples in the `examples/` [directory](https://github.com/lightbend/config/tree/master/examples).

You can run these from the sbt console with the commands `project
config-simple-app-java` and then `run`.

In brief, as shown in the examples:

 - libraries should use a `Config` instance provided by the app,
   if any, and use `ConfigFactory.load()` if no special `Config`
   is provided. Libraries should put their defaults in a
   `reference.conf` on the classpath.
 - apps can create a `Config` however they want
   (`ConfigFactory.load()` is easiest and least-surprising), then
   provide it to their libraries. A `Config` can be created with
   the parser methods in `ConfigFactory` or built up from any file
   format or data source you like with the methods in
   `ConfigValueFactory`.

### Immutability

Objects are immutable, so methods on `Config` which transform the
configuration return a new `Config`. Other types such as
`ConfigParseOptions`, `ConfigResolveOptions`, `ConfigObject`,
etc. are also immutable. See the
[API docs](https://lightbend.github.io/config/latest/api/) for
details of course.

### Schemas and Validation

There isn't a schema language or anything like that. However, two
suggested tools are:

 - use the
   [checkValid() method](https://lightbend.github.io/config/latest/api/com/typesafe/config/Config.html#checkValid-com.typesafe.config.Config-java.lang.String...-)
 - access your config through a Settings class with a field for
   each setting, and instantiate it on startup (immediately
   throwing an exception if any settings are missing)

In Scala, a Settings class might look like:

    class Settings(config: Config) {

        // validate vs. reference.conf
        config.checkValid(ConfigFactory.defaultReference(), "simple-lib")

        // non-lazy fields, we want all exceptions at construct time
        val foo = config.getString("simple-lib.foo")
        val bar = config.getInt("simple-lib.bar")
    }

See the examples/ directory for a full compilable program using
this pattern.

### Standard behavior

The convenience method `ConfigFactory.load()` loads the following
(first-listed are higher priority):

  - system properties
  - `application.conf` (all resources on classpath with this name)
  - `application.json` (all resources on classpath with this name)
  - `application.properties` (all resources on classpath with this
    name)
  - `reference.conf` (all resources on classpath with this name)

The idea is that libraries and frameworks should ship with a
`reference.conf` in their jar. Applications should provide an
`application.conf`, or if they want to create multiple
configurations in a single JVM, they could use
`ConfigFactory.load("myapp")` to load their own `myapp.conf`.
(Applications _can_ provide a `reference.conf` also if they want,
but you may not find it necessary to separate it from
`application.conf`.)

Libraries and frameworks should default to `ConfigFactory.load()`
if the application does not provide a custom `Config` object. This
way, libraries will see configuration from `application.conf` and
users can configure the whole app, with its libraries, in a single
`application.conf` file.

Libraries and frameworks should also allow the application to
provide a custom `Config` object to be used instead of the
default, in case the application needs multiple configurations in
one JVM or wants to load extra config files from somewhere.  The
library examples in `examples/` show how to accept a custom config
while defaulting to `ConfigFactory.load()`.

For applications using `application.{conf,json,properties}`,
system properties can be used to force a different config source
(e.g. from command line `-Dconfig.file=path/to/config-file`):

 - `config.resource` specifies a resource name - not a
   basename, i.e. `application.conf` not `application`
 - `config.file` specifies a filesystem path, again
   it should include the extension, not be a basename
 - `config.url` specifies a URL

These system properties specify a _replacement_ for
`application.{conf,json,properties}`, not an addition. They only
affect apps using the default `ConfigFactory.load()`
configuration. In the replacement config file, you can use
`include "application"` to include the original default config
file; after the include statement you could go on to override
certain settings.

If you set `config.resource`, `config.file`, or `config.url`
on-the-fly from inside your program (for example with
`System.setProperty()`), be aware that `ConfigFactory` has some
internal caches and may not see new values for system
properties. Use `ConfigFactory.invalidateCaches()` to force-reload
system properties.

#### Note about resolving substitutions in `reference.conf` and `application.conf`

The substitution syntax `${foo.bar}` will be resolved
twice. First, all the `reference.conf` files are merged and then
the result gets resolved. Second, all the `application.conf` are
layered over the `reference.conf` and the result of that gets
resolved again.

The implication of this is that the `reference.conf` stack has to
be self-contained; you can't leave an undefined value `${foo.bar}`
to be provided by `application.conf`, or refer to `${foo.bar}` in
a way that you want to allow `application.conf` to
override. However, `application.conf` can refer to a `${foo.bar}`
in `reference.conf`.

This can be frustrating at times, but possible workarounds
include:

  * putting an `application.conf` in a library jar, alongside the
`reference.conf`, with values intended for later resolution.
  * putting some logic in code instead of building up values in the
    config itself.

### Merging config trees

Any two Config objects can be merged with an associative operation
called `withFallback`, like `merged = firstConfig.withFallback(secondConfig)`.

The `withFallback` operation is used inside the library to merge
duplicate keys in the same file and to merge multiple files.
`ConfigFactory.load()` uses it to stack system properties over
`application.conf` over `reference.conf`.

You can also use `withFallback` to merge in some hardcoded values,
or to "lift" a subtree up to the root of the configuration; say
you have something like:

    foo=42
    dev.foo=57
    prod.foo=10

Then you could code something like:

    Config devConfig = originalConfig
                         .getConfig("dev")
                         .withFallback(originalConfig)

There are lots of ways to use `withFallback`.

### How to handle defaults

Many other configuration APIs allow you to provide a default to
the getter methods, like this:

    boolean getBoolean(String path, boolean fallback)

Here, if the path has no setting, the fallback would be
returned. An API could also return `null` for unset values, so you
would check for `null`:

    // returns null on unset, check for null and fall back
    Boolean getBoolean(String path)

The methods on the `Config` interface do NOT do this, for two
major reasons:

 1. If you use a config setting in two places, the default
 fallback value gets cut-and-pasted and typically out of
 sync. This can result in Very Evil Bugs.
 2. If the getter returns `null` (or `None`, in Scala) then every
 time you get a setting you have to write handling code for
 `null`/`None` and that code will almost always just throw an
 exception. Perhaps more commonly, people forget to check for
 `null` at all, so missing settings result in
 `NullPointerException`.

For most situations, failure to have a setting is simply a bug to fix
(in either code or the deployment environment). Therefore, if a
setting is unset, by default the getters on the `Config` interface
throw an exception.

If you want to allow a setting to be missing from
`application.conf` in a particular case, then here are some
options:

 1. Set it in a `reference.conf` included in your library or
 application jar, so there's a default value.
 2. Use the `Config.hasPath()` method to check in advance whether
 the path exists (rather than checking for `null`/`None` after as
 you might in other APIs).
 3. Catch and handle `ConfigException.Missing`. NOTE: using an
 exception for control flow like this is much slower than using
 `Config.hasPath()`; the JVM has to do a lot of work to throw
 an exception.
 4. In your initialization code, generate a `Config` with your
 defaults in it (using something like `ConfigFactory.parseMap()`)
 then fold that default config into your loaded config using
 `withFallback()`, and use the combined config in your
 program. "Inlining" your reference config in the code like this
 is probably less convenient than using a `reference.conf` file,
 but there may be reasons to do it.
 5. Use `Config.root()` to get the `ConfigObject` for the
 `Config`; `ConfigObject` implements `java.util.Map<String,?>` and
 the `get()` method on `Map` returns null for missing keys. See
 the API docs for more detail on `Config` vs. `ConfigObject`.
 6. Set the setting to `null` in `reference.conf`, then use
 `Config.getIsNull` and `Config.hasPathOrNull` to handle `null`
 in a special way while still throwing an exception if the setting
 is entirely absent.

The *recommended* path (for most cases, in most apps) is that you
require all settings to be present in either `reference.conf` or
`application.conf` and allow `ConfigException.Missing` to be
thrown if they are not. That's the design intent of the `Config`
API design.

Consider the "Settings class" pattern with `checkValid()` to
verify that you have all settings when you initialize the
app. See the [Schemas and Validation](#schemas-and-validation)
section of this README for more details on this pattern.

**If you do need a setting to be optional**: checking `hasPath()` in
advance should be the same amount of code (in Java) as checking
for `null` afterward, without the risk of `NullPointerException`
when you forget. In Scala, you could write an enrichment class
like this to use the idiomatic `Option` syntax:

```scala
implicit class RichConfig(val underlying: Config) extends AnyVal {
  def getOptionalBoolean(path: String): Option[Boolean] = if (underlying.hasPath(path)) {
     Some(underlying.getBoolean(path))
  } else {
     None
  }
}
```

Since this library is a Java library it doesn't come with that out
of the box, of course.

It is understood that sometimes defaults in code make sense. For
example, if your configuration lets users invent new sections, you
may not have all paths up front and may be unable to set up
defaults in `reference.conf` for dynamic paths. The design intent
of `Config` isn't to *prohibit* inline defaults, but simply to
recognize that it seems to be the 10% case (rather than the 90%
case). Even in cases where dynamic defaults are needed, you may
find that using `withFallback()` to build a complete
nothing-missing `Config` in one central place in your code keeps
things tidy.

Whatever you do, please remember not to cut-and-paste default
values into multiple places in your code. You have been warned!
:-)

### Understanding `Config` and `ConfigObject`

To read and modify configuration, you'll use the
[Config](https://lightbend.github.io/config/latest/api/com/typesafe/config/Config.html)
interface. A `Config` looks at a JSON-equivalent data structure as
a one-level map from paths to values. So if your JSON looks like
this:

```
  "foo" : {
    "bar" : 42
    "baz" : 43
  }
```

Using the `Config` interface, you could write
`conf.getInt("foo.bar")`. The `foo.bar` string is called a _path
expression_
([HOCON.md](https://github.com/lightbend/config/blob/master/HOCON.md)
has the syntax details for these expressions). Iterating over this
`Config`, you would get two entries; `"foo.bar" : 42` and
`"foo.baz" : 43`. When iterating a `Config` you will not find
nested `Config` (because everything gets flattened into one
level).

When looking at a JSON tree as a `Config`, `null` values are
treated as if they were missing. Iterating over a `Config` will
skip `null` values.

You can also look at a `Config` in the way most JSON APIs would,
through the
[ConfigObject](https://lightbend.github.io/config/latest/api/com/typesafe/config/ConfigObject.html)
interface. This interface represents an object node in the JSON
tree. `ConfigObject` instances come in multi-level trees, and the
keys do not have any syntax (they are just strings, not path
expressions). Iterating over the above example as a
`ConfigObject`, you would get one entry `"foo" : { "bar" : 42,
"baz" : 43 }`, where the value at `"foo"` is another nested
`ConfigObject`.

In `ConfigObject`, `null` values are visible (distinct from
missing values), just as they are in JSON.

`ConfigObject` is a subtype of [ConfigValue](https://lightbend.github.io/config/latest/api/com/typesafe/config/ConfigValue.html), where the other
subtypes are the other JSON types (list, string, number, boolean, null).

`Config` and `ConfigObject` are two ways to look at the same
internal data structure, and you can convert between them for free
using
[Config.root()](https://lightbend.github.io/config/latest/api/com/typesafe/config/Config.html#root--)
and
[ConfigObject.toConfig()](https://lightbend.github.io/config/latest/api/com/typesafe/config/ConfigObject.html#toConfig--).

### ConfigBeanFactory

As of version 1.3.0, if you have a Java object that follows
JavaBean conventions (zero-args constructor, getters and setters),
you can automatically initialize it from a `Config`.

Use
`ConfigBeanFactory.create(config.getConfig("subtree-that-matches-bean"),
MyBean.class)` to do this.

Creating a bean from a `Config` automatically validates that the
config matches the bean's implied schema. Bean fields can be
primitive types, typed lists such as `List<Integer>`,
`java.time.Duration`, `ConfigMemorySize`, or even a raw `Config`,
`ConfigObject`, or `ConfigValue` (if you'd like to deal with a
particular value manually).

## Using HOCON, the JSON Superset

The JSON superset is called "Human-Optimized Config Object
Notation" or HOCON, and files use the suffix `.conf`.  See
[HOCON.md](https://github.com/lightbend/config/blob/master/HOCON.md)
in this directory for more detail.

After processing a `.conf` file, the result is always just a JSON
tree that you could have written (less conveniently) in JSON.

### Features of HOCON

  - Comments, with `#` or `//`
  - Allow omitting the `{}` around a root object
  - Allow `=` as a synonym for `:`
  - Allow omitting the `=` or `:` before a `{` so
    `foo { a : 42 }`
  - Allow omitting commas as long as there's a newline
  - Allow trailing commas after last element in objects and arrays
  - Allow unquoted strings for keys and values
  - Unquoted keys can use dot-notation for nested objects,
    `foo.bar=42` means `foo { bar : 42 }`
  - Duplicate keys are allowed; later values override earlier,
    except for object-valued keys where the two objects are merged
    recursively
  - `include` feature merges root object in another file into
    current object, so `foo { include "bar.json" }` merges keys in
    `bar.json` into the object `foo`
  - include with no file extension includes any of `.conf`,
    `.json`, `.properties`
  - you can include files, URLs, or classpath resources; use
    `include url("http://example.com")` or `file()` or
    `classpath()` syntax to force the type, or use just `include
    "whatever"` to have the library do what you probably mean
    (Note: `url()`/`file()`/`classpath()` syntax is not supported
    in Play/Akka 2.0, only in later releases.)
  - substitutions `foo : ${a.b}` sets key `foo` to the same value
    as the `b` field in the `a` object
  - substitutions concatenate into unquoted strings, `foo : the
    quick ${colors.fox} jumped`
  - substitutions fall back to environment variables if they don't
    resolve in the config itself, so `${HOME}` would work as you
    expect. Also, most configs have system properties merged in so
    you could use `${user.home}`.
  - substitutions normally cause an error if unresolved, but
    there is a syntax `${?a.b}` to permit them to be missing.
  - `+=` syntax to append elements to arrays, `path += "/bin"`
  - multi-line strings with triple quotes as in Python or Scala

### Examples of HOCON

All of these are valid HOCON.

Start with valid JSON:

    {
        "foo" : {
            "bar" : 10,
            "baz" : 12
        }
    }

Drop root braces:

    "foo" : {
        "bar" : 10,
        "baz" : 12
    }

Drop quotes:

    foo : {
        bar : 10,
        baz : 12
    }

Use `=` and omit it before `{`:

    foo {
        bar = 10,
        baz = 12
    }

Remove commas:

    foo {
        bar = 10
        baz = 12
    }

Use dotted notation for unquoted keys:

    foo.bar=10
    foo.baz=12

Put the dotted-notation fields on a single line:

    foo.bar=10, foo.baz=12

The syntax is well-defined (including handling of whitespace and
escaping). But it handles many reasonable ways you might want to
format the file.

Note that while you can write HOCON that looks a lot like a Java
properties file (and many properties files will parse as HOCON),
the details of escaping, whitespace handling, comments, and so
forth are more like JSON. The spec (see HOCON.md in this
directory) has some more detailed notes on this topic.

### Uses of Substitutions

The `${foo.bar}` substitution feature lets you avoid cut-and-paste
in some nice ways.

#### Factor out common values

This is the obvious use,

    standard-timeout = 10ms
    foo.timeout = ${standard-timeout}
    bar.timeout = ${standard-timeout}

#### Inheritance

If you duplicate a field with an object value, then the objects
are merged with last-one-wins. So:

    foo = { a : 42, c : 5 }
    foo = { b : 43, c : 6 }

means the same as:

    foo = { a : 42, b : 43, c : 6 }

You can take advantage of this for "inheritance":

    data-center-generic = { cluster-size = 6 }
    data-center-east = ${data-center-generic}
    data-center-east = { name = "east" }
    data-center-west = ${data-center-generic}
    data-center-west = { name = "west", cluster-size = 8 }

Using `include` statements you could split this across multiple
files, too.

If you put two objects next to each other (close brace of the first
on the same line with open brace of the second), they are merged, so
a shorter way to write the above "inheritance" example would be:

    data-center-generic = { cluster-size = 6 }
    data-center-east = ${data-center-generic} { name = "east" }
    data-center-west = ${data-center-generic} { name = "west", cluster-size = 8 }

#### Optional system or env variable overrides

In default uses of the library, exact-match system properties
already override the corresponding config properties.  However,
you can add your own overrides, or allow environment variables to
override, using the `${?foo}` substitution syntax.

    basedir = "/whatever/whatever"
    basedir = ${?FORCED_BASEDIR}

Here, the override field `basedir = ${?FORCED_BASEDIR}` simply
vanishes if there's no value for `FORCED_BASEDIR`, but if you set
an environment variable `FORCED_BASEDIR` for example, it would be
used.

A natural extension of this idea is to support several different
environment variable names or system property names, if you aren't
sure which one will exist in the target environment.

Object fields and array elements with a `${?foo}` substitution
value just disappear if the substitution is not found:

    // this array could have one or two elements
    path = [ "a", ${?OPTIONAL_A} ]

### Concatenation

Values _on the same line_ are concatenated (for strings and
arrays) or merged (for objects).

This is why unquoted strings work, here the number `42` and the
string `foo` are concatenated into a string `42 foo`:

    key : 42 foo

When concatenating values into a string, leading and trailing
whitespace is stripped but whitespace between values is kept.

Unquoted strings also support substitutions of course:

    tasks-url : ${base-url}/tasks

A concatenation can refer to earlier values of the same field:

    path : "/bin"
    path : ${path}":/usr/bin"

Arrays can be concatenated as well:

    path : [ "/bin" ]
    path : ${path} [ "/usr/bin" ]

There is a shorthand for appending to arrays:

    // equivalent to: path = ${?path} [ "/usr/bin" ]
    path += "/usr/bin"

To prepend or insert into an array, there is no shorthand.

When objects are "concatenated," they are merged, so object
concatenation is just a shorthand for defining the same object
twice. The long way (mentioned earlier) is:

    data-center-generic = { cluster-size = 6 }
    data-center-east = ${data-center-generic}
    data-center-east = { name = "east" }

The concatenation-style shortcut is:

    data-center-generic = { cluster-size = 6 }
    data-center-east = ${data-center-generic} { name = "east" }

When concatenating objects and arrays, newlines are allowed
_inside_ each object or array, but not between them.

Non-newline whitespace is never a field or element separator. So
`[ 1 2 3 4 ]` is an array with one unquoted string element
`"1 2 3 4"`. To get an array of four numbers you need either commas or
newlines separating the numbers.

See the spec for full details on concatenation.

Note: Play/Akka 2.0 have an earlier version that supports string
concatenation, but not object/array concatenation. `+=` does not
work in Play/Akka 2.0 either. Post-2.0 versions support these
features.

### `reference.conf` can't refer to `application.conf`

Please see <a
href="#note-about-resolving-substitutions-in-referenceconf-and-applicationconf">this
earlier section</a>; all `reference.conf` have substitutions
resolved first, without `application.conf` in the stack, so the
reference stack has to be self-contained.

## Miscellaneous Notes

### Debugging Your Configuration

If you have trouble with your configuration, some useful tips.

 - Set the Java system property `-Dconfig.trace=loads` to get
   output on stderr describing each file that is loaded.
   Note: this feature is not included in the older version in
   Play/Akka 2.0.
 - Use `myConfig.root().render()` to get a `Config` printed out as a
   string with comments showing where each value came from.

### Supports Java 8 and Later

Currently the library is maintained against Java 8, but
version 1.2.1 and earlier will work with Java 6.

Please use 1.2.1 if you need Java 6 support, though some people
have expressed interest in a branch off of 1.3.x supporting
Java 7. If you want to work on that branch you might bring it up
on [chat](https://gitter.im/lightbend/config). We can release a
jar for Java 7 if someone(s) steps up to maintain the branch. The
master branch does not use Java 8 "gratuitously" but some APIs
that use Java 8 types will need to be removed.

### Rationale for Supported File Formats

(For the curious.)

The three file formats each have advantages.

 - Java `.properties`:
   - Java standard, built in to JVM
   - Supported by many tools such as IDEs
 - JSON:
   - easy to generate programmatically
   - well-defined and standard
   - bad for human maintenance, with no way to write comments,
     and no mechanisms to avoid duplication of similar config
     sections
 - HOCON/`.conf`:
   - nice for humans to read, type, and maintain, with more
     lenient syntax
   - built-in tools to avoid cut-and-paste
   - ways to refer to the system environment, such as system
     properties and environment variables

The idea would be to use JSON if you're writing a script to spit
out config, and use HOCON if you're maintaining config by hand.
If you're doing both, then mix the two.

Two alternatives to HOCON syntax could be:

  - YAML is also a JSON superset and has a mechanism for adding
    custom types, so the include statements in HOCON could become
    a custom type tag like `!include`, and substitutions in HOCON
    could become a custom tag such as `!subst`, for example. The
    result is somewhat clunky to write, but would have the same
    in-memory representation as the HOCON approach.
  - Put a syntax inside JSON strings, so you might write something
    like `"$include" : "filename"` or allow `"foo" : "${bar}"`.
    This is a way to tunnel new syntax through a JSON parser, but
    other than the implementation benefit (using a standard JSON
    parser), it doesn't really work. It's a bad syntax for human
    maintenance, and it's not valid JSON anymore because properly
    interpreting it requires treating some valid JSON strings as
    something other than plain strings. A better approach is to
    allow mixing true JSON files into the config but also support
    a nicer format.

### Other APIs (Wrappers, Ports and Utilities)

This may not be comprehensive - if you'd like to add mention of
your wrapper, just send a pull request for this README. We would
love to know what you're doing with this library or with the HOCON
format.

#### Guice integration
  * Typesafe Config Guice https://github.com/racc/typesafeconfig-guice

#### Java (yep!) wrappers for the Java library

  * tscfg https://github.com/carueda/tscfg

#### Scala wrappers for the Java library

  * Ficus https://github.com/ceedubs/ficus
  * configz https://github.com/arosien/configz
  * configs https://github.com/kxbmap/configs
  * config-annotation https://github.com/zhongl/config-annotation
  * PureConfig https://github.com/pureconfig/pureconfig
  * Simple Scala Config https://github.com/ElderResearch/ssc
  * konfig https://github.com/vpon/konfig
  * ScalaConfig https://github.com/andr83/scalaconfig
  * static-config https://github.com/Krever/static-config
  * validated-config https://github.com/carlpulley/validated-config
  * Cedi Config https://github.com/ccadllc/cedi-config
  * Cfg https://github.com/carueda/cfg
  * circe-config https://github.com/circe/circe-config

#### Clojure wrappers for the Java library

  * beamly-core.config https://github.com/beamly/beamly-core.config

#### Kotlin wrappers for the Java library
  * config4k https://github.com/config4k/config4k

#### Scala port

  * SHocon https://github.com/unicredit/shocon (work with both Scala and Scala.Js)

#### Ruby port

   * https://github.com/puppetlabs/ruby-hocon

#### Puppet module

   * Manage your HOCON configuration files with Puppet!: https://forge.puppetlabs.com/puppetlabs/hocon

#### Python port

   * pyhocon https://github.com/chimpler/pyhocon

#### C++ port

   * https://github.com/puppetlabs/cpp-hocon

#### JavaScript port

  * https://github.com/yellowblood/hocon-js (missing features, under development)

#### C# port

  * https://github.com/akkadotnet/HOCON

#### Linting tool

   * A web based linting tool http://www.hoconlint.com/
   
# Maintanance notes

## License

The license is Apache 2.0, see LICENSE-2.0.txt.

## Maintained by 

This project is maintained mostly by [@havocp](https://github.com/havocp) and [@akka-team](https://github.com/orgs/lightbend/teams/akka-team/members).

Feel free to ping above maintainers for code review or discussions. Pull requests are very welcome–thanks in advance!
