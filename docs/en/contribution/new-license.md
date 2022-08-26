# How To Add New License

### ASF 3RD PARTY LICENSE POLICY

You have to pay attention to the following open-source software protocols which Apache projects support when you intend to add a new feature to the SeaTunnel (or other Apache projects), which functions refers to other open-source software references.

[ASF 3RD PARTY LICENSE POLICY](https://apache.org/legal/resolved.html)

If the 3rd party software is not present at the above policy, we could't that accept your code.


### How to Legally Use 3rd Party Open-source Software in the SeaTunnel

Moreover, when we intend to refer a new software ( not limited to 3rd party jar, text, CSS, js, pics, icons, audios etc and modifications based on 3rd party files) to our project, we need to use them legally in addition to the permission of ASF. Refer to the following article:

* [COMMUNITY-LED DEVELOPMENT "THE APACHE WAY"](https://apache.org/dev/licensing-howto.html)


For example, we should contain the NOTICE file (most of open-source project has NOTICE file, generally under root directory) of ZooKeeper in our project when we are using ZooKeeper. As the Apache explains, "Work" shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work.

We are not going to dive into every 3rd party open-source license policy in here, you may look up them if interested.

### SeaTunnel-License Check Rules

In general, we would have our License-check scripts to our project. SeaTunnel-License-Check is provided by [SkyWalking](https://github.com/apache/skywalking) which differ a bit from other open-source projects. All in all, we are trying to make sure avoiding the license issues at the first time.

We need to follow the following steps when we need to add new jars or external resources:

* Add the name and the version of the jar file in the known-dependencies.txt
* Add relevant maven repository address under 'seatunnel-dist/release-docs/LICENSE' directory
* Append relevant NOTICE files under 'seatunnel-dist/release-docs/NOTICE' directory and make sure they are no different to the original repository
* Add relevant source code protocols under 'seatunnel-dist/release-docs/licenses' directory and the file name should be named as license+filename.txt. Eg: license-zk.txt

* check dependency license fail
```
--- /dev/fd/63 2020-12-03 03:08:57.191579482 +0000
+++ /dev/fd/62 2020-12-03 03:08:57.191579482 +0000
@@ -1,0 +2 @@
+HikariCP-java6-2.3.13.jar
@@ -16,0 +18 @@
+c3p0-0.9.5.2.jar
@@ -149,0 +152 @@
+mchange-commons-java-0.2.11.jar

- commons-lang-2.1.3.jar
Error: Process completed with exit code 1.
```

Generally speaking, the work of adding a jar is often not so easy to end, because it often depends on various other jars, and we also need to add corresponding licenses for these jars. In this case, we will get the error message of check dependency license fail in check. As above, we are missing the license declaration of `HikariCP-java6-2.3.13`, `c3p0`, etc. (`+` means new, `-` means need to delete ), follow the steps to add jar to add

### References

* [COMMUNITY-LED DEVELOPMENT "THE APACHE WAY"](https://apache.org/dev/licensing-howto.html)
* [ASF 3RD PARTY LICENSE POLICY](https://apache.org/legal/resolved.html)
