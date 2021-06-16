package decimalgamma;

import org.apache.spark.SparkConf;
import org.rumbledb.api.Item;
import org.rumbledb.api.Rumble;
import org.rumbledb.api.SequenceOfItems;
import org.rumbledb.config.RumbleRuntimeConfiguration;
import scala.util.Properties;
import sparksoniq.spark.SparkSessionManager;

public class Main {
    public static final String javaVersion = System.getProperty("java.version");
    public static final String scalaVersion = Properties.scalaPropOrElse("version.number", "unknown");
    protected static final RumbleRuntimeConfiguration configuration = new RumbleRuntimeConfiguration();

    public static void main(String[] args) {
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

        Rumble rumble = new Rumble(configuration);
        SequenceOfItems sequence = rumble.runQuery("for $a in (1, 2, 3) return $a");
        sequence.open();

        while (sequence.hasNext()) {
            Item next = sequence.next();
        }
    }
}