# X2SeaTunnel 转换报告

## 📋 基本信息

| 项目 | 值 |
|------|----| 
| **转换时间** | {{convertTime}} |
| **源文件** | `{{sourceFile}}` |
| **目标文件** | `{{targetFile}}` |
| **源类型** | {{sourceType}} |
| **目标类型** | SeaTunnel |
| **转换状态** | {{status}} |
{{customTemplateInfo}}
| **工具版本** | 1.0.0-SNAPSHOT (迭代1.3) |

{{errorInfo}}

## 📊 转换统计

| 类型 | 数量 | 百分比 |
|------|------|--------|
| ✅ **成功映射** | {{successCount}} | {{successPercent}} |
| 🔧 **自动构造** | {{autoCount}} | {{autoPercent}} |
| ❌ **缺失必填** | {{missingCount}} | {{missingPercent}} |
| ⚠️ **未映射** | {{unmappedCount}} | {{unmappedPercent}} |
| **总计** | {{totalCount}} | 100% |

## ✅ 成功映射的字段

{{successMappingTable}}

## 🔧 自动构造的字段

{{autoConstructedTable}}

## ❌ 缺失的必填字段

{{missingFieldsTable}}

## ⚠️ 未映射的字段

{{unmappedFieldsTable}}

## 💡 建议和说明

{{recommendations}}

### 📖 关于X2SeaTunnel

X2SeaTunnel是一个配置转换工具，当前版本 (迭代1.3) 实现了以下功能：

- ✅ {{sourceTypeName}} JSON配置解析
- ✅ 基础字段映射（MySQL、Oracle等JDBC源）
- ✅ SeaTunnel配置模板生成
- ✅ 详细的转换报告
{{customFeatures}}

**后续版本将支持**：
- 更多连接器类型
- 复杂数据类型映射
- 批量配置转换
- 配置验证功能

---
*报告生成时间: {{generateTime}}*
