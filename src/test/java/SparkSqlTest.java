
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.col;

public class SparkSqlTest {

    private static SparkSession spark;

    @BeforeAll
    static void beforeAll() {
        spark = SparkSession.builder().appName("SparkSqlTest").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp")
                .getOrCreate();
    }

    @AfterAll
    static void afterAll() {
        spark.close();
    }

    @Test
    @DisplayName("Testing Spark SQL datasets")
    void dataSetsTest() {
        try {
            Dataset<Row> dataSet = spark.read().option("header", true).csv("src/test/resources/students.csv");
            dataSet.show();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }

    @Test
    @DisplayName("Testing filters")
    void dataSetsFiltersTest() {
        try {
            Dataset<Row> dataset = spark.read().option("header", true).csv("src/test/resources/students.csv");
//            Dataset<Row> filterResults = dataset.filter("subject='Math' AND year > '2000'");
            Dataset<Row> filterResults = dataset.filter(
                    col("subject").equalTo("Math")
                            .and(col("year").geq(2007))
            );
            filterResults.show();
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}


