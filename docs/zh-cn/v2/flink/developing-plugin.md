# 插件开发


## 插件体系介绍

Waterdrop插件分为三部分，**Source**、**Transform**和**Sink**

### Source

**Source**负责将外部[数据源](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/dev/connectors/)的数据转化为`DataStream<T>`或者`DataSet<T>`

### Transform

**Transform**是`DataStream<T>`或者`DataSet<T>`做一些[转换操作](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/dev/stream/operators/)

### Sink

**Output**是负责将`DataStream<T>`或者`DataSet<T>输出到[外部数据源](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/dev/connectors/)或者打印到终端

## 准备工作

Waterdrop 支持Java/Scala作为插件开发语言，但是目前只能基于源代码开发，以后将会支持插件化开发

##  一、拉取源代码

```bash
git clone https://github.com/InterestingLab/waterdrop.git
```

## 二、 建立自己的plugin module

在waterdrop主目录下建立module,pom.xml引入

```
        <dependency>
            <groupId>io.github.interestinglab</groupId>
            <artifactId>waterdrop-flink-api</artifactId>
            <version>2.0.0</version>
            <scope>provided</scope>
        </dependency>
```

## 三、 实现自己的plugin

### Source(实时流)

- 新建一个类，并继承**Waterdrop-apis**提供的父类`FlinkStreamSource<T>`
    
 ```java
public class  MyStream implements FlinkStreamSource<T> {
     private Config config;
     @Override
     public void setConfig(Config config) {
            this.config = config;
     }
    
     @Override
     public Config getConfig() {
         return config;
     }

}

```
    
- 重写父类定义的`checkConfig`、`prepare`和`getData`方法
 ```java
public void prepare(FlinkEnvironment env);
public CheckResult checkConfig();
public DataStream<T> getData(FlinkEnvironment env);
```

- **Source**插件在调用时会先执行`checkConfig`方法核对调用插件时传入的参数是否正确，然后调用`prepare`方法配置参数的缺省值以及初始化类的成员变量，最后调用`getData`方法将外部数据源转换为`DataStream<T>`


### Transform

- 新建一个类，并继承**Waterdrop-apis**提供的父类`FlinkStreamTransform<IN, OUT>`
    ```Java
    public class MyTransform extends FlinkStreamTransform<IN, OUT> {
    
        private Config config;
    
        @Override
        public Config getConfig() {
            return config;
        }
    
        @Override
        public void setConfig(Config config) {
            this.config = config;
        }
    }
    ```

- 重写父类定义的`checkConfig`、`prepare`和`processStream`方法
 ```java
public void prepare(FlinkEnvironment env);
public CheckResult checkConfig();
DataStream<OUT> processStream(FlinkEnvironment env, DataStream<IN> dataStream);
```

- **Transform**插件在调用时会先执行`checkConfig`方法核对调用插件时传入的参数是否正确，然后调用`prepare`方法配置参数的缺省值以及初始化类的成员变量，最后调用`processStream`方法将外输入数据`DataStream<IN>`转换为`DataStream<OUT>`

### Sink

- 新建一个类，并继承**Waterdrop-apis**提供的父类`FlinkStreamSink<IN,OUT>`
    ```Java
    public class MySink extends FlinkStreamSink<IN,OUT> {
    
        private Config config;
    
        @Override
        public Config getConfig() {
            return config;
        }
    
        @Override
        public void setConfig(Config config) {
            this.config = config;
        }
    }
    ```
- 重写父类定义的`checkConfig`、`prepare`和`outputStream`方法

    ```Java
    @Override
    public void prepare(FlinkEnvironment env);
    public CheckResult checkConfig();
    @Override
    public DataStreamSink<OUT> outputStream(FlinkEnvironment env, DataStream<IN> dataStream);
    ```
    - **Sink**插件调用结构与**Transform**插件相似。在调用时会先执行`checkConfig`方法核对调用插件时传入的参数是否正确，然后调用`prepare`方法配置参数的缺省值以及初始化类的成员变量，最后调用`outputStream`方法将 **DataStream<OUT> dataStream** 格式数据输出到外部数据源。

## 四、 打包使用
将开发好的plugin 引入到waterdrop-core module下的pom.xml中

    > mvn package 将waterdrop-core-2.0.0-2.11.8.jar替换



