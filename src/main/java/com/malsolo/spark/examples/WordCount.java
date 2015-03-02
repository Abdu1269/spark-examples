package com.malsolo.spark.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class WordCount {
	
	private final static String INPUT_FILE_TEXT = "data/the_constitution_of_the_united_states.txt";
	private final static String OUTPUT_FILE_TEXT = "out";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count with Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile(INPUT_FILE_TEXT);
		
		final Accumulator<Integer> blankLines = sc.accumulator(0);
		
		final Broadcast<List<String>> wordsToIgnore = sc.broadcast(getWordsToIgnore());
		
		@SuppressWarnings("resource")
		JavaPairRDD<String, Integer> counts = lines.flatMap(line -> 
			{
				if ("".equals(line)) {
					blankLines.add(1);
				}
				return Arrays.asList(line.split(" "));
			})
			.filter(word -> !wordsToIgnore.value().contains(word))
			.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
			.reduceByKey((x, y) -> x + y);
		
		counts.saveAsTextFile(OUTPUT_FILE_TEXT);
		
		System.out.println("Blank lines: " + blankLines.value());
		
		sc.close();
	}

	private static List<String> getWordsToIgnore() {
		return Arrays.asList("the", "of", "and", "for");
	}

}
