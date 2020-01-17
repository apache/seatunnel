## start-waterdrop-flink.sh 使用方法


```bash
 bin/start-waterdrop-flink.sh -c config-path [other params]
```
> 使用 `-c`或者`--config`来指定配置文件的路径。其余参数参考flink原始参数，查看flink参数方法:`flink run -h`，参数可以根据需求任意添加，如`-m yarn-cluster`则指定为on yarn模式。 

```bash
flink run -h
```
* flink standalone 可配置的参数
![standalone](../../../images/flink/standalone.jpg)
例如：-p 2 指定作业并行度为2
```bash
   bin/start-waterdrop-flink.sh -p 2 -c config-path
```

* flink yarn-cluster 可配置参数
![yarn-cluster](../../../images/flink/yarn.jpg)
例如：-m yarn-cluster -ynm waterdrop 指定作业在运行在yarn上，并且yarn webUI的名称为waterdrop
```bash
   bin/start-waterdrop-flink.sh -m yarn-cluster -ynm waterdrop -c config-path
```