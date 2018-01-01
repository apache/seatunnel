# 插件开发


## 准备工作

新建一个Java/Scala项目，或者可以直接拉取[waterdrop-filter-example](https://github.com/InterestingLab/waterdrop-filter-example)，然后在此项目上进行修改

##  一、 新建pom.xml

参考文件[pom.xml](https://github.com/InterestingLab/waterdrop-filter-example/blob/master/pom.xml)

将`waterdrop`提供的接口加入项目的依赖中
```
<dependency>
    <groupId>io.github.interestinglab.waterdrop</groupId>
    <artifactId>waterdrop-apis_2.11</artifactId>
    <version>0.1.0</version>
</dependency>
```

## 二、 实现自己的方法

1. 新建一个类，并继承`waterdrop-apis`提供的父类`BaseFilter`
2. 重写父类定义的方法`checkConfig`、`prepare`和`process`方法

### 说明

- Java版本的实现参照[Javasubstring](https://github.com/InterestingLab/waterdrop-filter-example/blob/master/src/main/java/org/interestinglab/waterdrop_test/filter/Javasubstring.java)，Scala版本的实现参照[Scalasubstring](https://github.com/InterestingLab/waterdrop-filter-example/blob/master/src/main/java/org/interestinglab/waterdrop_test/filter/Scalasubstring.java)
- `checkConfig`方法负责核对插件参数是否正确输入
- `prepare`方法主要负责配置插件参数的缺省值
- `process`是插件实现的具体代码

## 三、 打包使用

1. 打包
> mvn package

2. 将打包好的Jar包放到`waterdrop`指定目录下
> plugins/your_plugin_name/lib/your_jar_name

3. 在配置文件中使用插件
    ```
    org.interestinglab.waterdrop_test.filter.Scalasubstring {
        source_field = "message"
        target_field = "tmp"
        pos = 0
        len = 3
    }
    ```