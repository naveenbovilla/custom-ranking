package com.rank.core;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AppWorking
{
    public static void main( String[] args ) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://username:password@0.0.0.0:27017/test.teams_snapshot_input")
                .config("spark.mongodb.output.uri", "mongodb://username:password@0.0.0.0:27017/test.teams_snapshot_final")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


        Instant start = Instant.now();
        JavaRDD<Document> rdd1 = MongoSpark.load(jsc);
        JavaPairRDD<Double, Document> pairs =
                rdd1.mapToPair(s -> {
                    return new Tuple2<>((Double) s.get("teamScore"), s);
                });

        JavaPairRDD<Double, Iterable<Document>> tem = pairs.groupByKey();
        AtomicInteger rank = new AtomicInteger(0);

        List<Document> updateRankList = new ArrayList<>();

        //JavaRDD<Document> samRDD = tem.map(v1 -> v1._2().spliterator().forEachRemaining(document -> document.size()));

//        Collection<Iterable<Document>> collection = tem.collectAsMap().values();

//        collection.stream().parallel().forEach(
//                i -> {
//                    Iterator<Document> it = i.iterator();
//                    Document doc;
//                    AtomicInteger tempRank = new AtomicInteger(0);
//                    while (it.hasNext()) {
//                        doc = it.next();
//                        doc.put("teamRank", rank.get() + tempRank.get());
//                        tempRank.getAndIncrement();
//                        updateRankList.add(doc);
//                    }
//                    //System.out.println("No of team with the above score " + tempRank.get());
//                    rank.getAndIncrement();
//                }
//
//        );

//        tem.collect().parallelStream().forEach(doubleIterableTuple2 -> {
//            Iterator<Document> it = doubleIterableTuple2._2.iterator();
//                    Document doc;
//                    AtomicInteger tempRank = new AtomicInteger(0);
//                    while (it.hasNext()) {
//                        doc = it.next();
//                        doc.put("teamRank", rank.get() + tempRank.get());
//                        tempRank.getAndIncrement();
//                        updateRankList.add(doc);
//                    }
//                    System.out.println("No of team with the above score " + tempRank.get());
//                    rank.getAndIncrement();
//        });

//        tem.collectAsMap().values().parallelStream().forEach(
//
//                i -> {
//                    Iterator<Document> it = i.iterator();
//                    Document doc;
//                    AtomicInteger tempRank = new AtomicInteger(0);
//                    while (it.hasNext()) {
//                        doc = it.next();
//                        doc.put("teamRank", rank.get() + tempRank.get());
//                        tempRank.getAndIncrement();
//                        updateRankList.add(doc);
//                    }
//                    System.out.println("No of team with the above score " + tempRank.get());
//                    rank.getAndIncrement();
//                }
//
//
//        );


//          Working piece

        for (Tuple2<Double, Iterable<Document>> tuple : tem.collect()) {
            AtomicInteger tempRank = new AtomicInteger(0);
            //System.out.println("Team scores " + tuple._1.toString());
            Iterator<Document> it = tuple._2.iterator();
            Document doc;

            while (it.hasNext()) {
                doc = it.next();
                doc.put("teamRank", rank.get() + tempRank.get());
                tempRank.getAndIncrement();
                updateRankList.add(doc);
            }
            //System.out.println("No of team with the above score " + tempRank.get());
            rank.getAndIncrement();
        }

        System.out.println("-------------" + updateRankList.size());
        JavaRDD<Document> updateRDD = jsc.parallelize(updateRankList);

        MongoSpark.save(updateRDD);
        updateRDD.collect();

        Instant finish = Instant.now();

        long timeElapsed = Duration.between(start, finish).toMillis();
        long seconds = TimeUnit.MILLISECONDS.toSeconds(timeElapsed);

        System.out.println("Total time to sort and rank ------------- :" + seconds);

        jsc.stop();
    }

}