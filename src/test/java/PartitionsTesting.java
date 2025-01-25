import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Scanner;

public class PartitionsTesting {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;

    @BeforeAll
    static void beforeAll() {
        sparkConf = new SparkConf()
                .setAppName("PartitionsTesting")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @Test
    @DisplayName("Testing partitions, narrow & wide transformations")
    void partitionsTesting() {
        try {
            JavaRDD<String> initialRdd = sparkContext.textFile("src/test/resources/biglog.txt");
            System.out.println("Initial Partitions: " + initialRdd.getNumPartitions());

            // Applying narrow transformations
            JavaPairRDD<String, String> warnsAgainstDate = initialRdd.mapToPair(row -> {
                String[] cols = row.split(",");
                String level = cols[0];
                String date = cols[1];
                return new Tuple2<>(level, date);
            });
            System.out.println("Partitions after narrow transformations: " + warnsAgainstDate.getNumPartitions());

            // Now time to do a wide transformations
            JavaPairRDD<String, Iterable<String>> results = warnsAgainstDate.groupByKey();
            System.out.println("Partitions after wide transformations: " + results.getNumPartitions());

            results = results.cache();

            results.collect().forEach(it -> System.out.println(it._1 + ": " + Iterables.size(it._2)));

            System.out.println(results.collect().size());

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            sparkContext.close();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}
