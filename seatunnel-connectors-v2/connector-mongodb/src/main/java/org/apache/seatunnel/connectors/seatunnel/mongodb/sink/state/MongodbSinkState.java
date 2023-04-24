package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class MongodbSinkState implements Serializable {
    List<WriteModel<BsonDocument>> writeModels;
}
