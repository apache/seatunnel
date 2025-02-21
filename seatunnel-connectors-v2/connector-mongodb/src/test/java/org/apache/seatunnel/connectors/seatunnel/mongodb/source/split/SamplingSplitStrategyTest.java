package org.apache.seatunnel.connectors.seatunnel.mongodb.source.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.mongodb.MongoNamespace;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SamplingSplitStrategyTest {

    @Mock
    private MongodbClientProvider clientProvider;

    @Mock
    private MongoCollection<BsonDocument> collection;

    @Mock
    private MongoDatabase database;

    @Mock
    private BsonDocument matchQuery;

    private SamplingSplitStrategy strategy;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        strategy = new SamplingSplitStrategy(clientProvider, "splitKey", null, null, 100L, 1000L);
        when(clientProvider.getDefaultCollection()).thenReturn(collection);
        when(clientProvider.getDefaultDatabase()).thenReturn(database);

        MongoNamespace namespace = new MongoNamespace("databaseName", "collectionName");
        when(collection.getNamespace()).thenReturn(namespace);
    }

    @Test
    public void testGetDocumentNumAndAvgSize() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        BsonDocument statsCmd = new BsonDocument("collStats", new BsonString("collectionName"));
        Document res = new Document();
        res.put("count", "1.3360484963E10");
        res.put("avgObjSize", 200.0);

        when(database.runCommand(statsCmd)).thenReturn(res);

        // The getDocumentNumAndAvgSize method is private, so we need to use reflection
        Method method = SamplingSplitStrategy.class.getDeclaredMethod("getDocumentNumAndAvgSize");
        method.setAccessible(true);
        ImmutablePair<Long, Long> result = (ImmutablePair<Long, Long>) method.invoke(strategy);

        assertEquals(Long.valueOf(13360484963L), result.getLeft());
        assertEquals(Long.valueOf(200), result.getRight());
    }
}