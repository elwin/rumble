package org.rumbledb.benchmark;


import org.apache.commons.cli.*;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.rumbledb.api.Item;
import org.rumbledb.api.Rumble;
import org.rumbledb.api.SequenceOfItems;
import org.rumbledb.config.RumbleRuntimeConfiguration;
import scala.util.Properties;
import sparksoniq.spark.SparkSessionManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

enum RunType {
    DEFAULT, DECIMAL_GAMMA, DATA_FRAME
}

public class Main {
    public static final String javaVersion = System.getProperty("java.version");
    public static final String scalaVersion = Properties.scalaPropOrElse("version.number", "unknown");
    protected static final RumbleRuntimeConfiguration configuration = new RumbleRuntimeConfiguration();
    protected static Rumble rumble;

    public static void main(String[] args) throws IOException, ParseException {

        Options options = new Options();
        options.addOption(Option
                .builder("f")
                .longOpt("file")
                .desc("path of query file")
                .hasArg(true)
                .required(true)
                .build()
        );

        options.addOption(Option
                .builder("t")
                .longOpt("type")
                .hasArg(true)
                .desc("Set type of execution {d, dg, df}")
                .build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String path = cmd.getOptionValue("file");

        RunType t;
        switch (cmd.getOptionValue("type")) {
            case "dg":
                t = RunType.DECIMAL_GAMMA;
                break;
            case "df":
                t = RunType.DATA_FRAME;
                break;
            default:
                t = RunType.DEFAULT;
                break;
        }

        do_something(path, t);
    }

    private static void do_something(String path, RunType type) throws IOException {
        System.err.println("Java version: " + javaVersion);
        System.err.println("Scala version: " + scalaVersion);

        SparkConf sparkConfiguration = new SparkConf();
        sparkConfiguration.setMaster("local[*]");
        sparkConfiguration.set("spark.submit.deployMode", "client");
        sparkConfiguration.set("spark.executor.extraClassPath", "lib/");
        sparkConfiguration.set("spark.driver.extraClassPath", "lib/");
        sparkConfiguration.set("spark.sql.crossJoin.enabled", "true"); // enables cartesian product

        // prevents spark from failing to start on MacOS when disconnected from the internet
        sparkConfiguration.set("spark.driver.host", "127.0.0.1");

        SparkSessionManager.getInstance().initializeConfigurationAndSession(sparkConfiguration, true);
        SparkSessionManager.COLLECT_ITEM_LIMIT = configuration.getResultSizeCap();
        System.err.println("Spark version: " + SparkSessionManager.getInstance().getJavaSparkContext().version());

        rumble = new Rumble(configuration);

        String query = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);

        switch (type) {
            case DECIMAL_GAMMA:
                RumbleRuntimeConfiguration.setUseDecimalGamma();
                break;
            case DATA_FRAME:
                query = query.replace("json-file", "structured-json-file");
                break;
        }

        long duration = run_query(query);
        System.out.println(DurationFormatUtils.formatDurationHMS(duration));
    }

    private static long run_query(String query) {
        long startTime = System.currentTimeMillis();

        SequenceOfItems sequence = rumble.runQuery(query);
        sequence.open();

        while (sequence.hasNext()) {
            Item next = sequence.next();
        }

        sequence.close();

        return System.currentTimeMillis() - startTime;
    }
}
