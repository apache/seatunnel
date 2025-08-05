# X2SeaTunnel 转换报告

## 📋 基本信息

| 项目 | 值 |
|------|----| 
| **转换时间** | {{convertTime}} |
| **源文件** | `{{sourceFile}}` |
| **目标文件** | `{{targetFile}}` |
| **源类型** | {{sourceType}} |
| **目标类型** | SeaTunnel |
| **源连接器** | {{sourceConnector}} |
| **目标连接器** | {{sinkConnector}} |
| **转换状态** | {{status}} |
{{customTemplateInfo}}
| **工具版本** | 0.1 |

{{errorInfo}}

## 📊 转换统计

| 类型 | 数量 | 百分比 |
|------|------|--------|
| ✅ **直接映射** | {{directCount}} | {{directPercent}} |
| 🔧 **转换映射** | {{transformCount}} | {{transformPercent}} |
| 🔄 **使用默认值** | {{defaultCount}} | {{defaultPercent}} |
| ❌ **缺失字段** | {{missingCount}} | {{missingPercent}} |
| ⚠️ **未映射** | {{unmappedCount}} | {{unmappedPercent}} |
| **总计** | {{totalCount}} | 100% |

## ✅ 直接映射的字段

{{directMappingTable}}

## 🔧 转换映射的字段

{{transformMappingTable}}

## 🔄 使用默认值的字段

{{defaultValuesTable}}

## ❌ 缺失的字段

{{missingFieldsTable}}

## ⚠️ 未映射的字段

{{unmappedFieldsTable}}
