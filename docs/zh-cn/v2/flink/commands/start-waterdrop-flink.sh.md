## start-waterdrop-flink.sh 使用方法


```bash
 bin/start-waterdrop-flink.sh -c config-path [other params]
```
> 使用 `-c`或者`--config`来指定配置文件的路径。其余参数参考flink原始参数，查看flink参数方法:`flink run -h`，参数可以根据需求任意添加，如`-m yarn-cluster`则指定为on yarn模式。 
