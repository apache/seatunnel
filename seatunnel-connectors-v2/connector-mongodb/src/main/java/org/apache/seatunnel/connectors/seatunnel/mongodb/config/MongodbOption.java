package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class MongodbOption {
    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MongoDB uri");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MongoDB database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MongoDB collection");

    public static final Option<String> MATCHQUERY =
            Options.key("matchQuery")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MatchQuery is a JSON string that specifies the selection criteria using query operators for the documents to be returned from the collection.\n");

    // Don't use now
    public static final String FORMAT = "format";

    // Don't use now
    public static final String DEFAULT_FORMAT = "json";
}
