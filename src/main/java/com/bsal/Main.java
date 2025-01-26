package com.bsal;

import com.bsal.ranking_courses.RankingCoursesJob;
import com.bsal.spark_streaming.LogStreamAnalysisJob;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
//        new RankingCoursesJob().run();
        new LogStreamAnalysisJob().run();
    }
}