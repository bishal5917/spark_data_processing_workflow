import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RddJoinsTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static List<Tuple2<Integer, Integer>> visitsRawData;
    private static List<Tuple2<Integer, String>> usersRawData;

    @BeforeAll
    static void beforeAll() {
        sparkConf = new SparkConf()
                .setAppName("CreateRDDUsingParallelizeTest")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        visitsRawData = new ArrayList<>();
        usersRawData = new ArrayList<>();
        visitsRawData.add(new Tuple2<>(4, 18));
        visitsRawData.add(new Tuple2<>(6, 4));
        visitsRawData.add(new Tuple2<>(10, 9));
        usersRawData.add(new Tuple2<>(1, "John"));
        usersRawData.add(new Tuple2<>(2, "Bob"));
        usersRawData.add(new Tuple2<>(3, "Alan"));
        usersRawData.add(new Tuple2<>(4, "Doris"));
        usersRawData.add(new Tuple2<>(5, "Marybelle"));
        usersRawData.add(new Tuple2<>(6, "Raquel"));
    }

    @Test
    @DisplayName("RDD Inner Join")
    void rddInnerJoin() {
        try {
            System.out.println("INNER JOIN");
            JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRawData);
            JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(usersRawData);
            JavaPairRDD<Integer, Tuple2<Integer, String>> joined = visits.join(users);
            joined.collect().forEach(System.out::println);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    @Test
    @DisplayName("RDD Left Outer Join")
    void rddLeftOuterJoin() {
        try {
            System.out.println("LEFT OUTER JOIN");
            JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRawData);
            JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(usersRawData);
            JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joined = visits.leftOuterJoin(users);
            joined.collect().forEach(System.out::println);
        } catch (Exception ignored) {
        }
    }

    @Test
    @DisplayName("RDD Right Outer Join")
    void rddRightOuterJoin() {
        try {
            System.out.println("RIGHT OUTER JOIN");
            JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRawData);
            JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(usersRawData);
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joined = visits.rightOuterJoin(users);
            joined.collect().forEach(System.out::println);
        } catch (Exception ignored) {
        }
    }
}
