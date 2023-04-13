 package org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde;

 import com.fasterxml.jackson.core.JsonProcessingException;
 import com.fasterxml.jackson.databind.ObjectMapper;
 import com.fasterxml.jackson.databind.node.ObjectNode;
 import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
 import org.apache.seatunnel.api.table.type.SeaTunnelRow;
 import org.bson.Document;

 public class RowDataDocumentSerializer implements DocumentSerializer<SeaTunnelRow> {

     private final RowDataToJsonConverters.RowDataToJsonConverter jsonConverter;

     private transient ObjectNode node;
    private final ObjectMapper mapper = new ObjectMapper();


    public RowDataDocumentSerializer(SeaTunnelDataType<?> type) {
        this.jsonConverter = new RowDataToJsonConverters()
                .createConverter(type);
    }
    @Override
    public Document serialize(SeaTunnelRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }
        try {
            jsonConverter.convert(mapper, node, row);
            String s = mapper.writeValueAsString(node);
            return Document.parse(s);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
 }
