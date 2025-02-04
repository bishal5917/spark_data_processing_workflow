package com.bsal.word_count;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class WordCountJob {
    public void run() {
        // setting up spark
        SparkConf conf = new SparkConf().setAppName("WordCountJob").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Reading the dataset
        JavaRDD<String> rdd = sc.textFile("src/test/resources/avicii_wake_me_up_lyrics.txt");
        // split them line by line
        JavaRDD<String> splitted = rdd.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
        // convert to lowercase
        splitted = splitted.map(String::toLowerCase).filter(w -> w != null && !w.trim().isEmpty());
        // Now time to do a reduce operation to count the values
        JavaPairRDD<String, Long> wordvsCount = splitted.mapToPair(
                val -> new Tuple2<>(val, 1L)
        );
        // now time to reduce by key
        wordvsCount = wordvsCount.reduceByKey(Long::sum);
        // Now gotta sort based on the counts
        JavaPairRDD<Long, String> countVsWord = wordvsCount.mapToPair(val -> new Tuple2<>(val._2, val._1));
        // sorting in descending order
        countVsWord = countVsWord.sortByKey(false);
        // preparing to save in a csv file
        JavaRDD<String> csvRDD = countVsWord.map(pair -> pair._2 + "," + pair._1);
        // saving to a local csv file
        saveOutputLocally(csvRDD, "target/avicii_lyrics_word_count_result");
        // printing
        countVsWord.collect().forEach(System.out::println);
    }

    private void saveOutputLocally(JavaRDD<String> rdd, String outputPath) {
        File outputDir = new File(outputPath);
        if (outputDir.exists()) {
            try {
                FileUtils.deleteDirectory(outputDir);
                System.out.println("Deleted existing output directory: " + outputPath);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        rdd.coalesce(1).saveAsTextFile(outputPath);
    }
}
