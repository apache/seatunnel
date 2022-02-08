# Sink plugin : Console [Flink]

## 描述

用于功能测试和调试，结果会在 taskManager 的 stdout 选项卡中输出

## 配置

| name           | Type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| common-options | string | no       | -             |

### common options [string]

Sink 插件常用参数，详见 [Sink Plugin](./sink-plugin.md)

## Examples

```bash
ConsoleSink{}
```

## Note

Flink 的控制台输出在 Flink 的 WebUI 中
