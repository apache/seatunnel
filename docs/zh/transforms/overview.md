---
slug: /transforms
---

# 数据转换总览

Transform 位于 source 和 sink 之间，用来做字段映射、过滤、SQL 处理、表级编排等中间加工。第一次上手时，不需要先把所有 transform 都看完；更合适的顺序是先确定数据从哪来、写到哪去，再回来选择需要的转换能力。

## 先按目标找入口

| 目标 | 推荐入口 |
| --- | --- |
| 先理解 transform 如何连接数据集 | [转换通用参数](./common-options/common-options.md) |
| 做行过滤或字段裁剪 | [数据过滤（Filter）](./filter.md) 和 [字段映射（Field Mapper）](./field-mapper.md) |
| 用 SQL 表达式处理数据 | [SQL 转换](./sql.md) 和 [SQL 函数](./sql-functions.md) |
| 重命名或重组字段 | [字段重命名（Field Rename）](./field-rename.md) 和 [字段拆分（Split）](./split.md) |
| 处理多表链路 | [多表转换（Transform Multi Table）](./transform-multi-table.md) 和 [表合并（Table Merge）](./table-merge.md) |

## 新用户推荐顺序

1. 先看通用参数页，把 `plugin_input` 和 `plugin_output` 的作用理解清楚。
2. 先选择最简单、最贴近目标的 transform，再进入 SQL 或多表编排。
3. 每次只新增一个 transform 步骤，让整条 pipeline 在验证时保持可读、可排障。
