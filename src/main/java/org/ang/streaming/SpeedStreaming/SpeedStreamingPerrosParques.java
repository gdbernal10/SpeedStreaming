/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ang.streaming.SpeedStreaming;

/**
 *
 * @author Administrator
 */
import java.util.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.*;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import org.apache.spark.streaming.Durations;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import org.bson.Document;
import static java.util.Arrays.asList;

public class SpeedStreamingPerrosParques {

    private static final Pattern SPACE = Pattern.compile(" ");
    public static final String LOG_PATH = "/home/vagrant/SpeedStreaming/logSpeedStreaming.txt";
    public static final String LOG_PATH2 = System.getenv("LOG_PATH_SL");
    public static final String MONGODB = "mongodb://10.1.1.110:27017"; 
    public static final String MONGODB2 =  System.getenv("MONGODB_SL"); 
    
    //public static final String LOG_PATH = "C:\\Users\\Administrator\\Dropbox\\Andes\\4208_Arquitectuta_NG\\taller5\\MaquinaIoT\\repositorio\\SpeedStreaming\\logSpeedStreaming.txt";

    public static void main(String[] args) throws Exception {
        //args = new String[]{"10.0.1.25:9092", "gps"};
        if (args.length < 2) {
            System.err.println("Usage: SpeedStreamingPerrosParques <broker> <topic>\n"
                    + "  <broker> is a Kafka broker\n"
                    + "  <topics> is a kafka topic to consume from\n\n");
            System.exit(1);
        }
        toFile(System.getenv("LOG_PATH_SL"),LOG_PATH);
        toFile(System.getenv("MONGODB_SL"),LOG_PATH);
        //saveToMongo("{\"id\":\"1\",\"type\":\"collar1\",\"pet_name\":\"Lazzy2\",\"lat\":\"100\",\"log\":\"67\",\"otro\":\"test\",\"medical\":\"injured\"}");

        System.out.println("Checkpoint 1");
        //StreamingExamples.setStreamingLogLevels();

        String brokers = args[0];
        String topics = args[1];

        System.out.println("Checkpoint 2");

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SpeedStreamingPerrosParques").setMaster("local[2]");
        System.out.println("Checkpoint 2.1");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        System.out.println("Checkpoint 3");

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        System.out.println("Checkpoint 4");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        System.out.println("Checkpoint 5");

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new org.apache.spark.api.java.function.Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                //toFile("Method: Main: Trace: " + "Line MQ: " + tuple2._2, LOG_PATH);
                saveToMongo(tuple2._2);
                return tuple2._2();
            }
        });
        lines.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public static void toFile(String data, String fileNameFullPath) throws IOException {      
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");		
	Calendar calendar = Calendar.getInstance();	
	//System.out.println("Date : " + sdf.format(calendar.getTime()));        
        List<String> lines = Arrays.asList(sdf.format(calendar.getTime()) + ": " + data);
        Path file = Paths.get(fileNameFullPath);
        if (file.toFile().exists()) {
            Files.write(file, lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
        } else {
            Files.write(file, lines, Charset.forName("UTF-8"));
        }
    }

    private static void saveToMongo(String jsonData) throws Exception {
        try {
            //toFile("Method: saveToMongo: Trace: " + "** BEGIN EXEC ** ", LOG_PATH);
            //MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://10.1.1.110:27017"));
            MongoClient mongoClient = new MongoClient(new MongoClientURI(MONGODB));
            MongoDatabase db = mongoClient.getDatabase("kml_db");
            //toFile("Method: saveToMongo: Trace: " + "db: " + db.getName(), LOG_PATH);
            //toFile("Method: saveToMongo: Trace: " + "db: " + db.listCollections(), LOG_PATH);
            JSONArray array = JsonDecode(jsonData);
            //toFile("Method: saveToMongo: Trace: " + "JSONArray: " + array.toString(), LOG_PATH);
            //toFile("Method: saveToMongo: Trace: " + "JSONObject: " + array.get(0).toString(), LOG_PATH);
            JSONObject data = (JSONObject) array.get(0);
            //toFile("Method: saveToMongo: Trace: " + "Data: " + data.toJSONString(), LOG_PATH);
            Document key = new Document("id_collar", data.get("id"));
            Document doc = new Document();
            doc.putAll(data);
            doc.append("id_collar", data.get("id"))
                    .append("createdAt", System.currentTimeMillis());
                    //.append("createdAt", System.currentTimeMillis()).remove("id");
            //toFile("Method: saveToMongo: Trace: " + "key: " + key.toJson(), LOG_PATH);
            //toFile("Method: saveToMongo: Trace: " + "Data Exists: " + db.getCollection("perros_loc").find(key).first(), SpeedStreamingPerrosParques.LOG_PATH);
            if (db.getCollection("perros_loc").find(key).first() == null) {
                db.getCollection("perros_loc").insertOne(doc);
            } else {
                db.getCollection("perros_loc").updateOne(key,new Document("$set",doc));
            }
            //toFile("Method: saveToMongo: Trace: " + "** END EXEC ** ", LOG_PATH);
        } catch (Exception e) {
            toFile("Method: saveToMongo, Exception: " + e.getMessage(), LOG_PATH);
        }
    }

    public static JSONArray JsonDecode(String jsonData) throws IOException {
        try {
            //toFile("Method: JsonDecode: Trace: " + "** BEGIN EXEC ** ", LOG_PATH);
            JSONParser parser = new JSONParser();
            //toFile("Method: JsonDecode: Trace: " + "Input:  jsonData: " + jsonData, LOG_PATH);
            jsonData = jsonData.indexOf("[") == 1 ? jsonData : "[" + jsonData + "]";
            //toFile("Method: JsonDecode: Trace: " + "Input:  jsonData - Final: " + jsonData, LOG_PATH);
            Object obj = parser.parse(jsonData);
            JSONArray array = (JSONArray) obj;
            //toFile("Method: JsonDecode: Trace: " + "** END EXEC ** ", LOG_PATH);
            return array;
        } catch (Exception pe) {
            toFile("Method: JsonDecode, Exception: " + pe.getMessage(), LOG_PATH);
            return null;
        }
    }
}
