# 为 Waterdrop 贡献代码 

## Coding Style

Scala Coding Style 参考: http://twitter.github.io/effectivescala/

使用sbt插件[scalastyle](http://www.scalastyle.org/)作为coding style检查工具；无法通过coding style检查的代码无法提交.


## 代码/文档贡献流程

* Interesting Lab成员 :

(1) 从 master上 checkout 出新分支，分支名称要求

(2) 开发, 提交commit

(3) 经至少1个其他成员审核通过后，merge到master分支中.

* 非Interesting Lab 成员(常见的github协作流程):

(1) 在Waterdrop主页 fork 这个项目 https://github.com/InterestingLab/waterdrop

(2) 开发

(3) 提交commit

(4) 在你自己的项目主页上，提交pull request

(5) Interesting Lab 审核通过后，你的贡献将被纳入项目代码中。

## 自动化Build与Test

每次master分支的更新，都会触发自动化Build与Test

## 国内sbt加速

```
# 增加全局 repositories 配置, 加速依赖下载

[repositories]
local
oschina:http://maven.oschina.net/content/groups/public/
oschina-ivy:http://maven.oschina.net/content/groups/public/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]
oschina-thirdparty:http://maven.oschina.net/content/repositories/thirdparty/
typesafe: http://repo.typesafe.com/typesafe/ivy-releases/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext], bootOnly
typesafe2: http://repo.typesafe.com/typesafe/releases/
sbt-plugin: http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/
sonatype: http://oss.sonatype.org/content/repositories/snapshots
uk_maven: http://uk.maven.org/maven2/
ibibli: http://mirrors.ibiblio.org/maven2/
repo2: http://repo2.maven.org/maven2/
```
