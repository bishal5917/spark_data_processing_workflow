import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRddTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static List<String> inputData;

    @BeforeAll
    static void beforeAll() {
        sparkConf = new SparkConf()
                .setAppName("RddFlatMapsTest")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        // initializing the inputData
        inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("FATAL: Tuesday 10 September 0405");
        inputData.add("ERROR: Tuesday 14 September 0405");
        inputData.add("WARN: Tuesday 24 September 0405");
        inputData.add("ERROR: Tuesday 24 September 0405");
        inputData.add("FATAL: Tuesday 7 September 0405");
    }

    @Test
    @DisplayName("Testing pair Rdd and reduce in verbose manner")
    void testPairRddAndReduceInVerboseWay() {
        try {
            //Creating the RDD and reducing by key and printing (Long way)
            JavaRDD<String> originalLogMessages = sparkContext.parallelize(inputData);
            JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(val -> {
                        String[] columns = val.split(":");
                        String level = columns[0];
                        return new Tuple2<>(level, 1L);
                    }
            );
            // Reducing by the key
            JavaPairRDD<String, Long> sumsRdd = pairRDD.reduceByKey(Long::sum);
            pairRDD.foreach(
                    tuple -> {
                        System.out.println(tuple._1 + " : " + tuple._2);
                    }
            );
            System.out.println("*************** After Reducing **************");
            sumsRdd.foreach(
                    tuple -> {
                        System.out.println(tuple._1 + " : " + tuple._2);
                    }
            );
            sparkContext.close();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing pair Rdd and reduce in easy way")
    void testPairRddAndReduceInEasyWay() {
        try {
            sparkContext.parallelize(inputData)
                    .mapToPair(val -> new Tuple2<String, Long>(val.split(":")[0], 1L))
                    .reduceByKey(Long::sum)
                    .foreach(tuple -> {
                        System.out.println(tuple._1 + " : " + tuple._2);
                    });
            sparkContext.close();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}

