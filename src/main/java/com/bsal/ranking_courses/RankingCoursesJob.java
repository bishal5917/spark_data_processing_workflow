package com.bsal.ranking_courses;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RankingCoursesJob {

    public void run() {
        SparkConf conf = new SparkConf().setAppName("RankingCoursesJob").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        boolean testMode = true;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titleData = setUpTitlesDataRdd(sc, testMode);

        // step-1: calculate total chapters for the each course
        JavaPairRDD<Integer, Integer> chaptersPerCourseRdd = chapterData
                .mapToPair(Tuple2::swap)
                .mapToPair(x -> new Tuple2<>(x._1, 1))
                .reduceByKey(Integer::sum);
//        chaptersPerCourse.collect().forEach(System.out::println);

        // remove any duplicated views
        viewData = viewData.distinct();

        // step-2: get the course ids into the RDD
        viewData = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1));
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = viewData.join(chapterData);

        //step-3: don't need chapterIds anymore, setting up for a reduce
        JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRdd.mapToPair(row -> {
                    Integer userId = row._2._1;
                    Integer courseId = row._2._2;
                    return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
                }
        );

        // step-4: count how many views for each users per course
        step3 = step3.reduceByKey(Long::sum);

        // step-5: Don't need any userIds
        JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> {
                    Integer courseId = row._1._2;
                    Long views = row._2;
                    return new Tuple2<>(courseId, views);
                }
        );

        // step-6: Add in the total chapter count
        JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chaptersPerCourseRdd);

        // step-7: Calculate the percentage (courseId, percentage)
        JavaPairRDD<Integer, Double> step7 = step6.mapToPair(row -> {
            Integer courseId = row._1;
            Double percentageWatched = ((double) row._2._1 / row._2._2);
            return new Tuple2<>(courseId, percentageWatched);
        });

        // step-8: Calculate the score (courseId, score)
        JavaPairRDD<Integer, Long> step8 = step7.mapToPair(row -> {
            Integer courseId = row._1;
            long score = 0L;
            Double percent = row._2;
            if (percent > 0.9) {
                score = 10L;
            } else if (percent > 0.5) {
                score = 4L;
            } else if (percent > 0.25) {
                score = 2L;
            }
            return new Tuple2<>(courseId, score);
        });

        // step-9 reduce by key to get the final total scores
        step8 = step8.reduceByKey(Long::sum);

        // step-10 reduce by key to get the final total scores (views,courseId)
        JavaPairRDD<Long, Integer> step10 = step8.mapToPair(row -> new Tuple2<>(row._2, row._1));

        // sort by key to get the one with the higher value first
        step10.sortByKey(false).collect().forEach(System.out::println);

        sc.close();
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            //chapterId, title
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/ranking_courses/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            //chapterId,courseId
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/ranking_courses/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - userId, chapterId
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/ranking_courses/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
