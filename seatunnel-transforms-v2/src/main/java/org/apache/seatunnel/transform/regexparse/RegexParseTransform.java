 package org.apache.seatunnel.transform.regexparse;

 import lombok.extern.slf4j.Slf4j;
 import org.apache.commons.lang3.StringUtils;
 import org.apache.commons.lang3.math.NumberUtils;
 import org.apache.seatunnel.api.configuration.ReadonlyConfig;
 import org.apache.seatunnel.api.table.catalog.Column;
 import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
 import org.apache.seatunnel.api.table.catalog.TableIdentifier;
 import org.apache.seatunnel.api.table.catalog.TableSchema;
 import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
 import org.apache.seatunnel.api.table.type.BasicType;
 import org.apache.seatunnel.api.table.type.SeaTunnelRow;
 import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.List;
 import java.util.Map;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;


 @Slf4j
 public class RegexParseTransform extends AbstractCatalogSupportMapTransform {
    private int fieldIndex = -1;
    private final Pattern regex;
    private final Map<String, String> groupMap;

    public RegexParseTransform(TableTransformFactoryContext context) {
        super(context.getCatalogTables().get(0));
        ReadonlyConfig options = context.getOptions();
        String regexParseField = options.get(RegexParseTransformConfig.REGEX_PARSE_FIELD);
        this.regex = Pattern.compile(options.get(RegexParseTransformConfig.REGEX));
        this.groupMap = options.get(RegexParseTransformConfig.GROUP_MAP);
        List<Column> columns = context.getCatalogTables().get(0).getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (regexParseField.equals(columns.get(i).getName())) {
                if (!BasicType.STRING_TYPE.equals(columns.get(i).getDataType())) {
                    throw new RuntimeException("regex_parse_field type must be string");
                }
                fieldIndex = i;
            }
        }
        if (fieldIndex == -1) {
            throw new RuntimeException("regex_parse_field not Contained");
        }
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> oldColumns = inputCatalogTable.getTableSchema().getColumns();
        List<Column> newColumns = new ArrayList<>();
        for (String key : groupMap.keySet()) {
            newColumns.add(PhysicalColumn.of(key, BasicType.STRING_TYPE, 200, true, null, ""));
        }
        newColumns.addAll(0, oldColumns);
        return TableSchema.builder().columns(newColumns).build();
    }

     @Override
     protected TableIdentifier transformTableIdentifier() {
         return inputCatalogTable.getTableId().copy();
     }

     @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        try {
            Object[] oldFields = inputRow.getFields();
            String rowValue = String.valueOf(inputRow.getField(fieldIndex));
            int groupSize = groupMap.size();
            Object[] merged = Arrays.copyOf(oldFields, oldFields.length + groupSize);
            Matcher matcher = regex.matcher(rowValue);
            if (StringUtils.isBlank(rowValue) || !matcher.matches()) {
                return new SeaTunnelRow(merged);
            }
            Object[] extracted = groupMap.values().stream()
                    .map(index -> matcher.group(NumberUtils.toInt(index)))
                    .toArray();
            System.arraycopy(extracted, 0, merged, oldFields.length, groupSize);
            return new SeaTunnelRow(merged);
        } catch (Exception e) {
            throw new RuntimeException("RegexParse->" + e.getMessage());
        }
    }

    @Override
    public String getPluginName() {
        return "RegexParse";
    }

 }
