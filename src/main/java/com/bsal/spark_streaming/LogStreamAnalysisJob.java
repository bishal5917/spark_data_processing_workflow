package com.bsal.spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalysisJob {
    public void run() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("LogStreamAnalysisJob").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);

        JavaDStream<String> results = inputData.map(item -> item);

        results.map(rawLogMessage -> rawLogMessage.split(",")[0])
                .mapToPair(logLevel -> new Tuple2<>(logLevel, 1L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(1))
                .print();

        sc.start();
        sc.awaitTermination();
    }
}
