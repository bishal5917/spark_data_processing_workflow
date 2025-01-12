import org.apache.commons.lang.RandomStringUtils;
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

public class RddMappingTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static List<String> data;

    @BeforeAll
    static void beforeAll() {
        sparkConf = new SparkConf()
                .setAppName("RddMappingTest")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        data = new ArrayList<>();
        final var datasize = 100_000;
        for (int i = 0; i < datasize; i++) {
            data.add(RandomStringUtils.randomAscii(ThreadLocalRandom.current().nextInt(10)));
        }
        assertEquals(datasize, data.size());
    }

    @Test
    @DisplayName("Testing spark mapping with count")
    void testSparkMappingWithCount() {
        try {
            final var rdd = sparkContext.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {
                final var strLens = rdd.map(String::length).count();
                assertEquals(data.size(), strLens);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.println("Spark RDD count method time taken: " + timeElapsed);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing spark mapping with collect")
    void testSparkMappingWithCollect() {
        try {
            final var rdd = sparkContext.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {
                final var strLens = rdd.map(String::length).collect();
                assertEquals(data.size(), strLens.size());
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.println("Spark RDD count method time taken: " + timeElapsed);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing spark mapping with reduce")
    void testSparkMappingWithReduce() {
        try {
            final var rdd = sparkContext.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {
                final var strLens = rdd.map(String::length).map(v->1L).reduce(Long::sum);
                assertEquals(data.size(), strLens);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.println("Spark RDD count method with reduce time taken: " + timeElapsed);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}
