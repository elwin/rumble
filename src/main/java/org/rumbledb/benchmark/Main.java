package org.rumbledb.benchmark;

import org.apache.commons.cli.*;
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
import java.util.logging.Logger;


enum RunType {
    DEFAULT, DECIMAL_GAMMA, DATA_FRAME
}


public class Main {
    public static final String javaVersion = System.getProperty("java.version");
    public static final String scalaVersion = Properties.scalaPropOrElse("version.number", "unknown");
    protected static final RumbleRuntimeConfiguration configuration = new RumbleRuntimeConfiguration();
    protected static Rumble rumble;

    public static void main(String[] args) throws IOException, ParseException {
        CommandLine cmd = processArgs(args);
        String path = cmd.getOptionValue("file");
        RunType type = parseType(cmd);

        startRumble();
        String query = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        query = prepareForBenchmark(type, query);

        // System.out.println(DurationFormatUtils.formatDurationHMS(measureQuery(query)));
        System.out.println(measureQuery(query));
    }

    private static RunType parseType(CommandLine cmd) {
        switch (cmd.getOptionValue("type", "default")) {
            case "decimalgamma":
                return RunType.DECIMAL_GAMMA;
            case "dataframe":
                return RunType.DATA_FRAME;
            case "default":
            default:
                return RunType.DEFAULT;
        }
    }

    private static CommandLine processArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(
            Option
                .builder("f")
                .longOpt("file")
                .desc("path of query file")
                .hasArg(true)
                .required(true)
                .build()
        );

        options.addOption(
            Option
                .builder("t")
                .longOpt("type")
                .hasArg(true)
                .desc("Set type of execution {decimalgamma, dataframe, default}")
                .build()
        );

        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static void startRumble() {
        Logger.getLogger("default").info("Java version: " + javaVersion);
        Logger.getLogger("default").info("Scala version: " + scalaVersion);

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
    }

    private static String prepareForBenchmark(RunType type, String query) {
        switch (type) {
            case DECIMAL_GAMMA:
                RumbleRuntimeConfiguration.setUseDecimalGamma();
                break;
            case DATA_FRAME:
                query = query.replace("json-file", "structured-json-file");
                break;
        }

        return query;
    }

    private static long measureQuery(String query) {
        long startTime = System.currentTimeMillis();

        SequenceOfItems sequence = rumble.runQuery(query);
        sequence.open();

        while (sequence.hasNext()) {
            Item next = sequence.next();
            // System.out.println(next);
        }

        sequence.close();

        return System.currentTimeMillis() - startTime;
    }
}
