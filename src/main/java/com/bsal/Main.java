package com.bsal;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession.builder()
                .appName("SparkApp")
                .master("local[*]")
                .getOrCreate();
             JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
            List<Integer> data = Stream.iterate(1, n -> n + 1)
                    .limit(4)
                    .collect(Collectors.toList());

            JavaRDD<Integer> myRdd = sc.parallelize(data);
            System.out.println("Total elements in RDD: " + myRdd.count());
            System.out.println("Default number of partitions: " + myRdd.getNumPartitions());

            Integer max = myRdd.reduce(Integer::max);
            Integer min = myRdd.reduce(Integer::min);
            Integer sum = myRdd.reduce(Integer::sum);
            System.out.println("MAX:" + max + " MIN:" + min + " SUM:" + sum);

//            try (Scanner scanner = new Scanner(System.in)) {
//                scanner.nextLine();
//            }
            while (true) {
                // Keeping the program running
            }
        }
    }
}