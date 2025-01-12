import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RddReduceTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;

    private static List<Double> data;

    @BeforeAll
    static void beforeAll() {
        sparkConf = new SparkConf()
                .setAppName("RddReduceTest")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        data = new ArrayList<>();
        final var datasize = 1_000_000;
        for (int i = 0; i < datasize; i++) {
            data.add(100 * ThreadLocalRandom.current().nextDouble() + 50);
        }
        assertEquals(datasize, data.size());
    }

    @Test
    @DisplayName("Testing reduce method")
    void testSparkReduceMethod() {
        try {
            final var rdd = sparkContext.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {
                final var sum = rdd.reduce(Double::sum);
                System.out.println("Total sum: " + sum);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.println("Time taken: " + timeElapsed + "ms");
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing fold method")
    void testSparkFoldMethod() {
        try {
            final var rdd = sparkContext.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {
                final var sum = rdd.fold(0D, Double::sum);
                System.out.println("Total sum: " + sum);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.println("Time taken: " + timeElapsed + "ms");
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing aggregate method")
    void testSparkAggregateMethod() {
        try {
            final var rdd = sparkContext.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {
                // calculate the sum by summing the sums of each partitions
                final var sumOfSums = rdd.aggregate(0D, Double::sum, Double::sum);
                // calculate the sum by summing the maxs of each partitions
                final var sumOfMaxs = rdd.aggregate(0D, Double::sum, Double::max);
                System.out.println("Sum of sums: " + sumOfSums);
                System.out.println("Sum of maxs: " + sumOfMaxs);
            }
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}
