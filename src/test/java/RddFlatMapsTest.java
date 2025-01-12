import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RddFlatMapsTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;

    @BeforeAll
    static void beforeAll() {
        sparkConf = new SparkConf()
                .setAppName("RddFlatMapsTest")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @Test
    @DisplayName("Testing flatmap method in spark RDD")
    void testSparkFlatMap() {
        try {
            String path = "src/test/resources/avicii_wake_me_up_lyrics.txt";
            final var lines = sparkContext.textFile(path);
            System.out.println("Total lines in file: " + lines.count());

            JavaRDD<List<String>> mappedLines = lines.map(
                    line -> List.of(line.split("\\s"))
            );
            System.out.println("Total lines in file: " + mappedLines.count());

            JavaRDD<String> words = lines.flatMap(
                    line -> List.of(line.split("\\s")).iterator());
            System.out.println("Total words in file: " + words.count());

            // Printing first ten words
            words.take(10).forEach(System.out::println);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing filter method in spark RDD with FlatMap")
    void testSparkFlatMapWithFilter() {
        try {
            String path = "src/test/resources/avicii_wake_me_up_lyrics.txt";
            final var lines = sparkContext.textFile(path);
            System.out.println("Total lines in file: " + lines.count());

            JavaRDD<String> words = lines.flatMap(
                    line -> List.of(line.split("\\s")).iterator());
            System.out.println("Total words before filtering: " + words.count());

            JavaRDD<String> filteredWords = words.filter(word -> word != null && (!word.trim().isEmpty()));
            System.out.println("Total words after filtering: " + filteredWords.count());

            // Printing first ten words
//            filteredWords.foreach(System.out::println); (Won't work, not serializable exception)
//            filteredWords.collect().forEach(System.out::println);
            filteredWords.take(10).forEach(System.out::println);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}
