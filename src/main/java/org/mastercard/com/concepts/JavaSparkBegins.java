package org.mastercard.com.concepts;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JavaSparkBegins {

    public static  void main(String[] args) {
        log.info("Hello, Java Spark!");
        List<Double> numbers = new ArrayList<>();
        numbers.add(5.12); numbers.add(6.23); numbers.add(7.0); numbers.add(8.12);
        numbers.add(9.0); numbers.add(10.04); numbers.add(11.45); numbers.add(12.67);
        SparkConf sparkConfig = new SparkConf().setAppName("Java Spark Basics").setMaster("local[*]");
        try(JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {
            JavaRDD<Double> numbersRDD = sparkContext.parallelize(numbers);
            double count = numbersRDD.reduce(Double::sum);
            log.info("Total Sum is: " + count);

            JavaRDD<Double> squareRdd = numbersRDD.map(num -> num * num);
            squareRdd.foreach(num -> log.info("Square Value: " + num));

            //Tuples2
            List<Integer> nums = new ArrayList<>();
            nums.add(1); nums.add(27); nums.add(36); nums.add(42); nums.add(5);
            nums.add(64);nums.add(72); nums.add(8);
            JavaRDD<Integer> numsRDD = sparkContext.parallelize(nums);
            JavaRDD<Tuple2<Integer,Double>> sqrtRdd = numsRDD.map(i -> new Tuple2<>(i, Math.sqrt(i)));
            sqrtRdd.foreach(tuple -> log.info("number: [{}] sqrt is [{}]", tuple._1, tuple._2));
        } catch (Exception e) {
            log.error("Error occurred while processing Spark RDD: ", e);
        }

    }
}
