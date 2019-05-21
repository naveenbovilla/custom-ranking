package com.rank.core;

import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;

/**
 * Java MongoDB : Insert a Document
 *
 */
public class InsertDocumentApp {


    private static DecimalFormat df2 = new DecimalFormat("###.##");

    public static void main(String[] args) {

        MongoClientURI uri = new MongoClientURI("mongodb://username:password@0.0.0.0:27017/?authSource=test");

        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("test");



        try {

//            Mongo mongo = new Mongo("localhost", 27017);
//            DB db = mongo.getDB("test");
//
            MongoCollection collection = database.getCollection("teams_snapshot_input");
            //DBCollection collection = db.getCollection("teams_snapshot_input");
            List<Document> listToInsert = new ArrayList<Document>();



            IntStream.range(1, 200001).forEach(

                    nbr -> {

                        Random r = new Random();
                        int randId =  r.ints(0, (99 + 1)).findFirst().getAsInt();
                        int capRandId =  r.ints(0, (99 + 1)).findFirst().getAsInt();

                        double random = ThreadLocalRandom.current().nextDouble(250, 800);


                        Map<String, Object> documentMap = new HashMap<String, Object>();
                        documentMap.put("id",nbr);
                        documentMap.put("teamId",nbr);
                        documentMap.put("contestId",randId);
                        documentMap.put("teamScore",Double.parseDouble(df2.format(random)));
                        documentMap.put("teamRank",0);
                        documentMap.put("teamName",nbr+"IdName");
                        documentMap.put("captainId",capRandId);
                        documentMap.put("capName","cap"+capRandId);
                        documentMap.put("vcId",capRandId+1);
                        documentMap.put("vcName","vcap"+(capRandId+1));
                        documentMap.put("misc-1","");
                        documentMap.put("misc-1","");


                        listToInsert.add(new Document(documentMap));

                    }

            );

            // 3. Map example
//            System.out.println("Map example...");
//            Map<String, Object> documentMap = new HashMap<String, Object>();
//            documentMap.put("id",12);
//            documentMap.put("teamId",1);
//            documentMap.put("contestId",10);
//            documentMap.put("teamScore",230.76);
//            documentMap.put("teamRank",0);
//            documentMap.put("teamName","12IdName");
//            documentMap.put("captainId",123);
//            documentMap.put("capName","cap123");
//            documentMap.put("vcId",1232);
//            documentMap.put("vcName","vcap123");
//            documentMap.put("misc-1","");
//            documentMap.put("misc-1","");
//
            collection.insertMany(listToInsert);
//
//            DBCursor cursorDocMap = collection.find();
//            while (cursorDocMap.hasNext()) {
//                System.out.println(cursorDocMap.next());
//            }






        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
