package decimalgamma;

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

public class Main {
    public static final String javaVersion = System.getProperty("java.version");
    public static final String scalaVersion = Properties.scalaPropOrElse("version.number", "unknown");
    protected static final RumbleRuntimeConfiguration configuration = new RumbleRuntimeConfiguration();
    protected static Rumble rumble;

    public static void main(String[] args) throws IOException {
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
        RumbleRuntimeConfiguration.setUseDecimalGamma();

        // String query = "count(json-file(\"src/test/resources/datasets/students.json\"))";
        // String query = "for $i in json-file(\"src/test/resources/datasets/confusion/confusion-2014-03-02.json\")
        // group by $i.guess return $i.guess";

        String path = "src/test/resources/benchmark/queries/query.jq";

        String query = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);


        long duration = run_query(query);
        System.out.println(DurationFormatUtils.formatDurationHMS(duration));
    }

    public static long run_query(String query) {
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
