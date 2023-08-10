package org.apache.seatunnel.connectors.seatunnel.elasticsearch;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchDataTypeConvertor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: nijiahui
 * @Date: 2023/8/10 14:19
 * @Description:
 * @Version： 1.0
 */
public class ElasticsearchSourceTest {
    @Test
    public void testPrepareWithEmptySource() throws PrepareFailException {
        List<String> source = Lists.newArrayList();

        Map<String, String> esFieldType = new HashMap<>();
        esFieldType.put("field1", "String");

        SeaTunnelRowType rowTypeInfo = null;
        ElasticSearchDataTypeConvertor elasticSearchDataTypeConvertor =
                new ElasticSearchDataTypeConvertor();
        if (CollectionUtils.isEmpty(source)) {
            List<String> keys = new ArrayList<>(esFieldType.keySet());
            SeaTunnelDataType[] fieldTypes = new SeaTunnelDataType[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                String esType = esFieldType.get(keys.get(i));
                SeaTunnelDataType seaTunnelDataType =
                        elasticSearchDataTypeConvertor.toSeaTunnelType(esType);
                fieldTypes[i] = seaTunnelDataType;
            }
            rowTypeInfo = new SeaTunnelRowType(keys.toArray(new String[0]), fieldTypes);
        }
        Assertions.assertNotNull(rowTypeInfo);
    }


}
