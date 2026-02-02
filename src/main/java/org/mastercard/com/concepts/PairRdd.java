package org.mastercard.com.concepts;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class PairRdd {
    public static void main(String[] args) {

        List<String> dataList = new ArrayList<>();
        dataList.add("warn: tues 4 sept 2024");
        dataList.add("error: fri 29 sept 2024");
        dataList.add("info: mon 12 oct 2024");
        dataList.add("warn: wed 14 oct 2024");
        dataList.add("fatal: fri 16 oct 2024");
        dataList.add("error: sun 18 oct 2024");
        dataList.add("debug: tue 20 oct 2024");
        dataList.add("info: thu 22 oct 2024");
        SparkConf sparkConfig = new SparkConf().setAppName("Pair RDD Basics").setMaster("local[*]");
        try(JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {
            JavaRDD<String> javardd = sparkContext.parallelize(dataList);
            JavaPairRDD<String, String> pairRdd = javardd.mapToPair(logger -> {
                String[] values = logger.split(":");
                return new Tuple2<>(values[0].trim(),values[1].trim());
            });
            pairRdd.foreach(tuple -> log.info("level [{}] corresponding text [{}]", tuple._1,  tuple._2));

            //groupbykey: this causes perf issues
            JavaPairRDD<String, Iterable<String>> groupedRdd = pairRdd.groupByKey();
            groupedRdd.foreach(tuple -> log.info("groupBykey: level [{}] count is [{}]", tuple._1,  tuple._2));

            //reduceByKey both of them are similar
            // o/p level [warn] count is [tues 4 sept 2024,wed 14 oct 2024]
            JavaPairRDD<String, String> reduceByKeyRdd = pairRdd.reduceByKey((text1, text2) -> text1 + "," + text2);
            reduceByKeyRdd.foreach(tuple -> log.info("reduceByKey: level [{}] count is [{}]", tuple._1,  tuple._2));

            JavaPairRDD<String, Integer> countByKeyRdd = pairRdd.mapValues(value -> 1).reduceByKey(Integer::sum);
            countByKeyRdd.foreach(tuple -> log.info("countByKey: level [{}] count is [{}]", tuple._1,  tuple._2));

            Map<String, Long> countByKeyRdd2 = pairRdd.countByKey();
            log.info("map of countByKey: {}", countByKeyRdd2);


        } catch (Exception e) {
            log.error("Error occurred while processing Spark RDD: ", e);
        }
    }
}
