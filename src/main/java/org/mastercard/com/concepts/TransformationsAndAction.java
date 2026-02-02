package org.mastercard.com.concepts;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.List;
import java.util.Map;

@Slf4j
public class TransformationsAndAction {

    public static void main(String[] args) {

        SparkConf sparkConfig = new SparkConf().setAppName("Transformations & Actions").setMaster("local[*]");
        try(JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {

            JavaRDD<String> userRdd = sparkContext.textFile("src/main/resources/users.txt");
            JavaRDD<String> eventsRdd = sparkContext.textFile("src/main/resources/events.txt");
            JavaRDD<String> ordersRdd = sparkContext.textFile("src/main/resources/orders.txt");

            //to verify files loaded properly
            userRdd.collect().forEach(line -> log.info("User Line: {}", line));
            eventsRdd.collect().forEach(eventsLine -> log.info("Events Line: {}", eventsLine));
            ordersRdd.collect().forEach(orderLine -> log.info("Order Line: {}", orderLine));

            //clean and parse
            JavaRDD<String> cleanUserRdd = userRdd.filter(line -> line != null && !line.isEmpty());
            JavaRDD<String> cleanEventsRdd = eventsRdd.filter(line -> line != null && !line.isEmpty());
            JavaRDD<String> cleanOrdersRdd = ordersRdd.filter(line -> line != null && !line.isEmpty());

            //map to Pair RDD
            JavaPairRDD<String, Tuple2<String, String>> usersTuple3 = cleanUserRdd.mapToPair(line -> {
               String[] values = line.split(",");
               return new Tuple2<>(values[0].trim(), new Tuple2<>(values[1].trim(), values[2].trim()));
            });

            JavaPairRDD<String, Tuple3<String, String, String>> eventsTuple3 = cleanEventsRdd.mapToPair(line -> {
               String[] values = line.split(",");
               return new Tuple2<>(values[0].trim(), new Tuple3<>(values[1].trim(), values[2].trim(), values[3].trim()));
            });

            JavaRDD<Tuple4<String, String, String, Double>> ordersTuple4 = cleanOrdersRdd.map(line -> {
               String[] values = line.split(",");
               return new Tuple4<>(values[0].trim(), values[1].trim(), values[2].trim(),
                       Double.parseDouble(values[3].trim()));
            });



            usersTuple3.foreach(tuple -> log.info("UserID: [{}] Name: [{}] country: [{}]", tuple._1, tuple._2._1, tuple._2._2));
            eventsTuple3.foreach(tuple -> log.info("EventDate: [{}] UserID: [{}] EventType: [{}] productId: [{}]",
                    tuple._1, tuple._2._1(),tuple._2._2(), tuple._2._3()));
            ordersTuple4.foreach(tuple -> log.info("OrderID: [{}] UserID: [{}] productId: [{}] amount: [{}]",
                    tuple._1(), tuple._2(), tuple._3(), tuple._4()));

            //count
            log.info("Total users: [{}] total events [{}] total orders [{}]",
                    usersTuple3.count(), eventsTuple3.count(), ordersTuple4.count());

            //distinct user appearing in view events
            long distinctUsers = eventsTuple3.filter(tuple -> tuple._2()._2().equalsIgnoreCase("View"))
                    .distinct().count();
            List<String> disinctUserIds = eventsTuple3.filter(t -> t._2()._2().equalsIgnoreCase("View"))
                    .map(t -> t._2()._1()).distinct().collect();
            log.info("Distinct users appeared in view events: [{}] and userIds are [{}]", distinctUsers, disinctUserIds);

            //views per user
            Map<String, Long> viewsPerUser = eventsTuple3.filter(t -> t._2()._2().equalsIgnoreCase("View"))
                    .mapToPair(t -> new Tuple2<>(t._2()._1(),1)).countByKey();

            log.info("Views per user: {}", viewsPerUser);

            //events per user and event type
            JavaRDD<Tuple2<String,String>> userEventKey = eventsTuple3.map(t -> new Tuple2<>(t._2()._1(), t._2()._2()));
            Map<Tuple2<String, String>, Long> eventsPerUserAndType = userEventKey
                    .mapToPair(t -> new Tuple2<>(t,1)).countByKey();
            log.info("Events per user and event type: {}", eventsPerUserAndType);

            //amount spent per user on purchases with country names
            JavaPairRDD<String, Tuple3<String,String,Double>> orderTuple3ForJoins = ordersTuple4
                    .mapToPair(t -> new Tuple2<>(t._2(), new Tuple3<>(t._1(), t._3(), t._4())));
            JavaPairRDD<String, Tuple2<Tuple2<String, String>, Tuple3<String, String, Double>>> joinedRdd =
                    usersTuple3.join(orderTuple3ForJoins);
            JavaRDD<Tuple3<String, String, Double>> userCountryAmountRdd = joinedRdd
                    .map(t -> new Tuple3<>(t._2()._1()._1, t._2()._1()._2, t._2()._2()._3()));

            userCountryAmountRdd.foreach(tuple -> log.info("User: [{}] Country: [{}] Amount Spent: [{}]",
                    tuple._1(), tuple._2(), tuple._3()));

            //country and amount spent per country
            /*JavaPairRDD<String, Tuple2<Tuple2<String, String>, Tuple3<String, String, Double>>> joinedRdd1 =
                    usersTuple3.join(orderTuple3ForJoins);
            JavaRDD<Tuple2<String, Double>> amountPerCountryRdd = joinedRdd1.map(t -> new Tuple2<>(t._2()._1()._2, t._2()._2()._3()));
            Map<String, Double> totalAmountPerCountry = amountPerCountryRdd
                    .mapToPair(t -> new Tuple2<>(t._1(), t._2()))
                    .reduceByKey(Double::sum).collectAsMap();*/

            Map<String, Double> totalAmountPerCountry = joinedRdd
                    .map(t -> new Tuple2<>(t._2()._1()._2, t._2()._2()._3()))
                    .mapToPair(t -> new Tuple2<>(t._1(), t._2()))
                    .reduceByKey(Double::sum).collectAsMap();

            log.info("Total amount spent per country: {}", totalAmountPerCountry);




        } catch(Exception e) {
            log.error("Error occurred while processing Spark RDD: ", e);
        }
    }
}
