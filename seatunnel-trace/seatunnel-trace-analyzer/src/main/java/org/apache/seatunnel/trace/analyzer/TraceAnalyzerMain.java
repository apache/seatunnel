/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.trace.analyzer;

import org.apache.seatunnel.trace.analyzer.model.BottleneckPoint;
import org.apache.seatunnel.trace.analyzer.model.TraceRecord;
import org.apache.seatunnel.trace.analyzer.model.TraceStatistics;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/** CLI entry point that reads trace files and generates a bottleneck report. */
@Slf4j
public class TraceAnalyzerMain {
    /** Parses CLI options, loads trace files, runs aggregation, and writes the HTML report. */
    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                formatter.printHelp("trace-analyzer", options);
                return;
            }

            String inputDir = cmd.getOptionValue("input", "/tmp/seatunnel/traces");
            String outputFile = cmd.getOptionValue("output", "trace-report.html");
            String jobId = cmd.getOptionValue("job");
            String date = cmd.getOptionValue("date");

            System.out.println("SeaTunnel StainTrace Analyzer");
            System.out.println("==============================");
            System.out.println("Input directory: " + inputDir);
            System.out.println("Output file: " + outputFile);
            if (jobId != null) {
                System.out.println("Job ID filter: " + jobId);
            }
            if (date != null) {
                System.out.println("Date filter: " + date);
            }
            System.out.println();

            System.out.print("Reading trace files... ");
            TraceFileReader reader = new TraceFileReader();
            List<TraceRecord> records = reader.readTraces(inputDir, jobId, date);
            System.out.println("OK (" + records.size() + " records)");

            if (records.isEmpty()) {
                System.out.println("No trace records found. Please check:");
                System.out.println("  1. Input directory exists: " + inputDir);
                System.out.println("  2. Trace files are in JSON Lines format");
                System.out.println("  3. Job ID and date filters are correct");
                return;
            }

            System.out.print("Analyzing data... ");
            TraceDataAggregator aggregator = new TraceDataAggregator();
            TraceStatistics stats = aggregator.aggregate(records);
            boolean doBottleneck = cmd.hasOption("bottleneck");
            List<BottleneckPoint> bottlenecks =
                    doBottleneck ? aggregator.analyzeBottlenecks(records) : Collections.emptyList();
            System.out.println("OK");

            System.out.print("Generating HTML report... ");
            HtmlReportGenerator generator = new HtmlReportGenerator();
            generator.generate(stats, bottlenecks, records, outputFile);
            System.out.println("OK");

            System.out.println();
            System.out.println("Analysis Summary:");
            System.out.println("  Total Traces: " + stats.getTotalCount());
            System.out.println(
                    "  Avg Latency: " + String.format("%.2f", stats.getAvgLatencyMs()) + " ms");
            System.out.println("  P95 Latency: " + stats.getP95LatencyMs() + " ms");
            System.out.println("  P99 Latency: " + stats.getP99LatencyMs() + " ms");
            System.out.println("  Max Latency: " + stats.getMaxLatencyMs() + " ms");
            System.out.println("  Min Latency: " + stats.getMinLatencyMs() + " ms");
            if (doBottleneck) {
                System.out.println("  Bottlenecks: " + bottlenecks.size());
            }
            System.out.println();
            System.out.println("Report generated successfully!");
            System.out.println("Open in your browser:");
            System.out.println("  file://" + new java.io.File(outputFile).getAbsolutePath());

        } catch (ParseException e) {
            System.err.println("Parsing failed: " + e.getMessage());
            formatter.printHelp("trace-analyzer", options);
            System.exit(1);
        } catch (Exception e) {
            log.error("Analysis failed", e);
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Declares the command-line interface for the standalone trace analyzer. */
    private static Options buildOptions() {
        Options options = new Options();

        options.addOption(
                "i",
                "input",
                true,
                "Input directory containing trace files (default: /tmp/seatunnel/traces)");
        options.addOption(
                "o", "output", true, "Output HTML file path (default: trace-report.html)");
        options.addOption("j", "job", true, "Filter by job ID (optional)");
        options.addOption("d", "date", true, "Filter by date in yyyy-MM-dd format (optional)");
        options.addOption(
                "b",
                "bottleneck",
                false,
                "Enable bottleneck analysis between consecutive stages (default: disabled)");
        options.addOption("h", "help", false, "Print this help message");

        return options;
    }
}
