# 如何添加新的 License

### ASF 第三方许可政策

如果您打算向SeaTunnel（或其他Apache项目）添加新功能，并且该功能涉及到其他开源软件引用的时候，请注意目前 Apache 项目支持遵从以下协议的开源软件。

[ASF 第三方许可政策](https://apache.org/legal/resolved.html)

如果您所使用的第三方软件并不在以上协议之中，那么很抱歉，您的代码将无法通过审核，建议您找寻其他替代方案。

### 如何在 SeaTunnel 中合法使用第三方开源软件

当我们想要引入一个新的第三方软件(包含但不限于第三方的 jar、文本、CSS、js、图片、图标、音视频等及在第三方基础上做的修改)至我们的项目中的时候，除了他们所遵从的协议是 Apache 允许的，另外一点很重要，就是合法的使用。您可以参考以下文章

* [COMMUNITY-LED DEVELOPMENT "THE APACHE WAY"](https://apache.org/dev/licensing-howto.html)

举个例子，当我们使用了 ZooKeeper，那么我们项目就必须包含 ZooKeeper 的 NOTICE 文件（每个开源项目都会有 NOTICE 文件，一般位于根目录），用Apache的话来讲，就是 "Work" shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work.

关于具体的各个开源协议使用协议，在此不做过多篇幅一一介绍，有兴趣可以自行查询了解。

### SeaTunnel-License 检测规则

通常情况下， 我们会为项目添加 License-check 脚本。 跟其他开源项目略有不同，SeaTunnel 使用 [SkyWalking](https://github.com/apache/skywalking) 提供的 SeaTunnel-License-Check。 总之，我们试图第一时间避免 License 问题。

当我们需要添加新的 jar 包或者使用外部资源时， 我们需要按照以下步骤进行操作：

* 在 known-dependencies.txt 文件中添加 jar 的名称和版本
* 在 'seatunnel-dist/release-docs/LICENSE' 目录下添加相关 maven 仓库地址
* 在 'seatunnel-dist/release-docs/NOTICE' 目录下添加相关的 NOTICE 文件， 并确保他们跟原来的仓库中的文件没有区别
* 在 'seatunnel-dist/release-docs/licenses' 目录下添加相关源码协议文件， 并且文件命令遵守 license-filename.txt 规则。 例：license-zk.txt
* 检查依赖的 license 是否出错

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

一般来说，添加一个 jar 的工作通常不是很容易，因为 jar 通常依赖其他各种 jar， 我们还需要为这些 jar 添加相应的许可证。 在这种情况下， 我们会收到检查 license 失败的错误信息。像上面的例子，我们缺少 `HikariCP-java6-2.3.13`, `c3p0` 等的 license 声明（`+` 表示新添加，`-` 表示需要删除）， 按照步骤添加 jar。

### 参考

* [COMMUNITY-LED DEVELOPMENT "THE APACHE WAY"](https://apache.org/dev/licensing-howto.html)
* [ASF 第三方许可政策](https://apache.org/legal/resolved.html)

