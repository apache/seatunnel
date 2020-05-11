## Compiling and Building Waterdrop Source Code



### How to deploy waterdrop artifacts to central maven repo

References:
1. https://central.sonatype.org/pages/producers.html
2. https://central.sonatype.org/pages/working-with-pgp-signatures.html
3. http://tutorials.jenkov.com/maven/publish-to-central-maven-repository.html
4. https://www.jannikarndt.de/blog/2017/09/releasing_a_scala_maven_project_to_maven_central/

Command:

```
mvn clean deploy
```

Note: If you encounter such error:

```
gpg: signing failed: Operation cancelled
```

please execute deploy command as follows:

```
export GPG_TTY=$(tty); mvn clean deploy
```

