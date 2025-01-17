import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

public class RddUsingExternalDatasetsTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;

    @BeforeAll
    static void beforeAll(){
        sparkConf = new SparkConf()
                .setAppName("CreateRDDUsingParallelizeTest")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @ParameterizedTest
    @ValueSource(strings =
            {"src\\test\\resources\\1000_random_words.txt"})
    @DisplayName("Test loading local test files into spark RDD")
    void testLoadingLocalTextFilesIntoSparkRDD(final String localFilePath){
        try{
            final var rdd = sparkContext.textFile(localFilePath);
            System.out.println("Total lines in the file " + rdd.count());
            rdd.take(10).forEach(System.out::println);
        }catch (Exception ex){
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Loading local csv file into spark RDD")
    void testLoadingLocalCSVFileIntoSparkRdd(){
        try{
            final var rdd = sparkContext.textFile("src/test/resources/country_capital.csv");
            System.out.println("Total lines in the file " + rdd.count());
            System.out.println(rdd.first());
            rdd.take(10).forEach(System.out::println);
        }catch (Exception ex){
            System.out.println("Exception: " + ex.getMessage());
        }
    }

}
