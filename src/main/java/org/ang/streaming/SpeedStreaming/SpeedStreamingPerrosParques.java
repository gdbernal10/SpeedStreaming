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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import org.apache.spark.streaming.Durations;

public class SpeedStreamingPerrosParques{
  
  private static final Pattern SPACE = Pattern.compile(" ");
  
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: SpeedStreamingPerrosParques <brokers> <topics>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }
    System.out.println("Checkpoint 1");
    //StreamingExamples.setStreamingLogLevels();

    String brokers = args[0];
    String topics = args[1];
    
    System.out.println("Checkpoint 2");
    
    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("SpeedStreamingPerrosParques");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
    
     System.out.println("Checkpoint 3");
    
    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);
    
     System.out.println("Checkpoint 4");
    
    //JavaPairInputDStream<String, String> messages1 = KafkaUtils.createStream(jssc, topics, topics, map);
     
     
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
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    
     System.out.println("Checkpoint 6");
    
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(SPACE.split(x));
      }
    });
    
     System.out.println("Checkpoint 7");
    
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(
        new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });
    
     System.out.println("Checkpoint 8");
    
    wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
