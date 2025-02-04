package com.bsal;

import com.bsal.spark_streaming_with_kafka.RecordAnalysisJob;
import com.bsal.word_count.WordCountJob;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
//        new RankingCoursesJob().run();
//        new LogStreamAnalysisJob().run();
//        new RecordAnalysisJob().run();
        new WordCountJob().run();
    }
}