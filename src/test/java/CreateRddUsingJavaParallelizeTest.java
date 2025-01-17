import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateRddUsingJavaParallelizeTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;

    @BeforeAll
    static void beforeAll(){
       sparkConf = new SparkConf()
                .setAppName("CreateRDDUsingParallelizeTest")
                .setMaster("local[*]");
       sparkContext = new JavaSparkContext(sparkConf);
    }

    @Test
    @DisplayName("Create an empty RDD with no partitions in spark")
    void createEmptyRddWithNoPartitionInSpark(){
        try{
            final var emptyRdd = sparkContext.emptyRDD();
            System.out.println(emptyRdd);
            System.out.println("Number of partitions " + emptyRdd.getNumPartitions());
        }catch (Exception ignored){
        }
    }

    @Test
    @DisplayName("Create an empty RDD with default partitions in spark")
    void createEmptyRddWithDefaultPartitionInSpark(){
        try{
            final var emptyRdd = sparkContext.parallelize(List.of());
            System.out.println(emptyRdd);
            System.out.println("Number of partitions " + emptyRdd.getNumPartitions());
        }catch (Exception ignored){
        }
    }

    @Test
    @DisplayName("Create an empty RDD with default partitions in spark")
    void createSparkRddWithDefaultPartitionInSpark(){
        try{
            // create the list
            List<Integer> dataInRdd = new ArrayList<>(Arrays.asList(1,2,3,4,5,6,7,8));
            final var myRdd = sparkContext.parallelize(dataInRdd);
            System.out.println(myRdd);
            System.out.println("Number of partitions: " + myRdd.getNumPartitions());
            System.out.println("Total Count: " + myRdd.count());
            myRdd.collect().forEach(System.out::println);
        }catch (Exception ignored){
        }
    }
}
