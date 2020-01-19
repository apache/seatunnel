# 为 Waterdrop v2.x 贡献代码

## Coding Style

Waterdrop v2.x的主要编程语言为：Java，包括流程代码，Flink插件；部分Spark插件仍然沿用v1.x的scala代码。

* Java Coding Style 参考：

Google Java Coding Style: https://google.github.io/styleguide/javaguide.html

* Scala Coding Style 参考:

http://docs.scala-lang.org/style/

https://github.com/databricks/scala-style-guide

使用sbt插件[scalastyle](http://www.scalastyle.org/)作为coding style检查工具；无法通过coding style检查的代码无法提交.

通过scalafmt利用[Cli或者IntelliJ Idea](http://scalameta.org/scalafmt/#IntelliJ)自动完成scala代码的格式化。
如果使用scalafmt的Idea插件，请在插件安装完后设置`文件保存时自动更正代码格式`,方法 "Preferences" -> "Tools" -> "Scalafmt", 勾选"format on file save"

## 项目代码编译运行

Waterdrop v2.x 放弃了sbt，改为社区用户期待已久的maven来做项目的管理，国内用户再也不用痛苦于sbt依赖下载很慢的问题了。

## 代码/文档贡献流程

* Interesting Lab成员(内部协作流程):

(1) 从 master上 checkout 出新分支，分支名称要求新功能: 
<username>.fea.<feature_name>，修复bug: <username>.fixbug.<bugname_or_issue_id>, 文档：<username>.doc.<doc_name>

(2) 开发, 提交commit

(3) 在github的项目主页，选中你的分支，点"new pull request"，提交pull request

(3) 经至少1个其他成员审核通过，并且travis-ci的build全部通过后，由审核人merge到master分支中.

(4) 删除你的分支

* 非Interesting Lab 成员(常见的github协作流程):

(1) 在Waterdrop主页 fork 这个项目 https://github.com/InterestingLab/waterdrop

(2) 开发

(3) 提交commit

(4) 在你自己的项目主页上，点"new pull request"，提交pull request

(5) Interesting Lab 审核通过后，你的贡献将被纳入项目代码中。

## 自动化Build与Test

此项目使用 [travis-ci](https://travis-ci.org/) 作为自动化Build工具.

所有分支每次commit有更新，都会触发自动化Build，新的pull request也会触发。

