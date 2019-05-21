package com.rank.core;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.BasicDBObject;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.Document;
import com.mongodb.spark.*;
import scala.Tuple2;

import javax.print.Doc;

public class App
{
    public static void main( String[] args )
    {

        /**
        System.out.println( "Hello World!" );
//        SparkConf sparkCfg = new SparkConf();
//        sparkCfg.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.teams_snapshot");
//        sparkCfg.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.teams_snapshot_o");
//        sparkCfg.set("spark.mongodb.input.maxChunkSize", "1");
//        JavaSparkContext jsc = new JavaSparkContext(sparkCfg);


        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.teams_snapshot")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.teams_snapshot_o")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        HashMap calcDependencies =  getDependencies();
        Broadcast<HashMap> broadcastedDependencies = jsc.broadcast(calcDependencies);

        JavaRDD<Document> rdd1 = MongoSpark.load(jsc);

        JavaPairRDD<Double, Document> pairs =
                rdd1.mapToPair(s -> {
                    return new Tuple2<Double, Document>((Double) s.get("teamScore"), s);
                });

        JavaPairRDD<Double, Iterable<Document>> tem = pairs.groupByKey();

        AtomicInteger rank = new AtomicInteger(0);

        List<Document> tt = new ArrayList<>();

        for (Tuple2<Double, Iterable<Document>> tuple : tem.collect()) {
            AtomicInteger tempRank = new AtomicInteger(0);
            System.out.println("Team scores "+tuple._1.toString());
            Iterator<Document> it = tuple._2.iterator();
            Document doc = null;

            while (it.hasNext()) {
                doc= it.next();
                doc.put("teamRank",rank.get() + tempRank.get());
                //doc.put("teamRank",9);
                tempRank.getAndIncrement();
                tt.add(doc);
            }
            System.out.println("No of team with the above score "+tempRank.get());
            rank.getAndIncrement();
        }

        JavaRDD<Document> ff = jsc.parallelize(tt);

        MongoSpark.save(ff);
        ff.collect();

            //doubleIterableTuple2._2().iterator().next().put("teamRank","5")).collect();

//        JavaRDD newRDD = pairs.map(x -> x._2);
//
//        MongoSpark.save(newRDD);
        //newRDD.collect();


//        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);
//        JavaPairRDD<Object, Iterable<Document>> rdd1 = mongoRDD
//                .mapToPair(team -> new Tuple2<Object, Document>(team.get("teamScore"), team))
//                .groupByKey()
//                .sortByKey();


//        JavaMongoRDD<Document> mongoRDD = MongoSpark.load(jsc);
//        JavaPairRDD<Object, Document> rdd = mongoRDD
//                .mapToPair(team -> new Tuple2<Object, Document>(team.get("teamScore"), team))
//                .sortByKey();
//
//        System.out.println(rdd.top(10));

//        JavaRDD<Document> rdd = MongoSpark.load(jsc);
        //Dataset<Row> teamDF = spark.createDataFrame(rdd,Document.class );
        //RelationalGroupedDataset myDataSet = teamDF.sort(org.apache.spark.sql.functions.col("teamScore")).groupBy(org.apache.spark.sql.functions.col("teamScore"));
//        JavaPairRDD<Double, Iterable<Document>> rddY = rdd.groupBy(i -> (Double)i.get("teamScore")).sortByKey(false);

        //System.out.println(rddY.collect());
        //List<Tuple2<Double, Iterable<Document>>> temp = rddY.collect();

//        AtomicInteger rank = new AtomicInteger(0);
//        List<Document> finalList = new ArrayList<>();
//        JavaPairRDD<Double, List<Document>> pairs = rddY.mapToPair(f -> {
//            AtomicInteger tempRank = new AtomicInteger(0);
//            f._2().forEach( document -> {
//                document.put("teamRank",0);
//                tempRank.getAndIncrement();
//                finalList.add(document);
//                    });
//            rank.set(rank.get() + tempRank.get());
//
//            return new Tuple2<>(f._1(), finalList);
//        });



//        MongoSpark.save(newRDD);
//        newRDD.collect();


//        JavaPairRDD<String, Integer> output = pairs.mapValues(f -> {
//            String[] tokens = f.split(";");
//            Integer a = Integer.parseInt(tokens[0]);
//            Integer b = Integer.parseInt(tokens[1]);
//            return a + b;
//        });
//
//        List<Tuple2<String, Integer>> res = output.collect();
//        for(Tuple2 t : res){
//            System.out.println(t._1 + "," + t._2);
//        }



//        Map<Object, Iterable<Document>> resultMap = temp.stream()
//                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2,(x,y)->{
//                    return x;
//                }));




//        List<Document> finalList = new ArrayList<>();
//        Stream<Object> teamRank;
//        teamRank = temp.stream().map(i -> {
//            AtomicInteger tempRank = new AtomicInteger(0);
//            Iterable<Document> documents = i._2;
//
//            for (Document d : documents) {
//                d.put("teamRank", rank);
//                tempRank.getAndIncrement();
//                finalList.add(d);
//            }
//
//            rank.set(rank.get() + tempRank.get());
//        });


//        System.out.println(finalList.toArray());

//        JavaRDD<Document> rdd = MongoSpark.load(jsc)
//                .map(doc -> {
//                    Document mdoc = (Document) doc;
//                    mdoc = calculateFare(mdoc);
//                    return mdoc;
//                });
        //MongoSpark.save(rdd);
        //rdd.collect();
        jsc.stop(); **/
    }
    private static HashMap getDependencies(){
        return new HashMap(); // do some checkint beforeproduction!
    }
    private static Document calculateFare(Document input){
        input.put("fare", 100);
        return input;
    }
}