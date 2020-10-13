## Compiling and Building Waterdrop Source Code


### How to Build Binary Release Package


```
mvn clean package
```

then, you will see waterdrop release zip file in `waterdrop-dist/target/`, such as `waterdrop-dist-2.0.4-2.11.8-release.zip`

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

#### GPG Signing Notes:

1. If you encounter such error:

```
gpg: signing failed: Operation cancelled
```

please execute deploy command as follows:

```
export GPG_TTY=$(tty); mvn clean deploy
```

2. If you encounter such error:

```
gpg: signing failed: Timeout
```

please remove current gpg files and regenerate it:

```
$ rm -rf ~/.gnupg
```

then regenerate:

https://central.sonatype.org/pages/working-with-pgp-signatures.html

3. If you encounter such error:

```
gpg: keyserver receive failed: No route to host
```

please find a available gpg host, such as: hkp://keyserver.ubuntu.com

gpg --keyserver hkp://keyserver.ubuntu.com --send-keys <your_key>

4. If you encounter such error:

```
No public key: Key with id: (xxxx) was not able to be located on http://keyserver.ubuntu.com:port. Upload your public key and try the operation again.
```

please use the keyserver in error log to send keys:

```
gpg --keyserver hkp://keyserver.ubuntu.com --send-keys <your_key>
```

then, it works.





