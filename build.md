## Compiling and Building Waterdrop Source Code



### How to deploy waterdrop artifacts to central maven repo

References:
1. https://central.sonatype.org/pages/producers.html
2. https://central.sonatype.org/pages/working-with-pgp-signatures.html
3. http://tutorials.jenkov.com/maven/publish-to-central-maven-repository.html

Command:

```
mvn clean deploy
```

Note: If you encounter such error:

please execute deploy command as follows:

```
export GPG_TTY=$(tty); mvn clean deploy
```

