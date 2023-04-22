package org.apache.seatunnel.example.spark.v2;

import org.apache.seatunnel.core.starter.exception.CommandException;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;

public class SeatunnelStreamingExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        String configurePath = args.length > 0 ? args[0] : "/examples/spark.streaming.conf";
        ExampleUtils.builder(configurePath);
    }
}
