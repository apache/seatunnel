# X2SeaTunnel Conversion Report

## 📋 Basic Information

| Item | Value |
|------|----|
| **Conversion Time** | {{convertTime}} |
| **Source File** | `{{sourceFile}}` |
| **Target File** | `{{targetFile}}` |
| **Source Type** | {{sourceType}} |
| **Target Type** | SeaTunnel |
| **Source Connector** | {{sourceConnector}} |
| **Target Connector** | {{sinkConnector}} |
| **Conversion Status** | {{status}} |
{{customTemplateInfo}}
| **Tool Version** | 0.1 |

{{errorInfo}}

## 📊 Conversion Statistics

| Type | Count | Percentage |
|------|------|--------|
| ✅ **Direct Mapping** | {{directCount}} | {{directPercent}} |
| 🔧 **Transform Mapping** | {{transformCount}} | {{transformPercent}} |
| 🔄 **Default Values Used** | {{defaultCount}} | {{defaultPercent}} |
| ❌ **Missing Fields** | {{missingCount}} | {{missingPercent}} |
| ⚠️ **Unmapped** | {{unmappedCount}} | {{unmappedPercent}} |
| **Total** | {{totalCount}} | 100% |

## ✅ Direct Mapped Fields

{{directMappingTable}}

## 🔧 Transform Mapped Fields

{{transformMappingTable}}

## 🔄 Fields Using Default Values

{{defaultValuesTable}}

## ❌ Missing Fields

{{missingFieldsTable}}

## ⚠️ Unmapped Fields

{{unmappedFieldsTable}}
