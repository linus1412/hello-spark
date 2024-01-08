package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class Main {

    private static final Pattern SPACE = Pattern.compile(" ");
    public static final String NON_WORD_CHARS = "[^a-zA-Z']";

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        var sparkConf = new SparkConf().setAppName("JavaWordCount");
        try (var ctx = new JavaSparkContext(sparkConf)) {

            var sortedCounts = wordsByCount(ctx.textFile(args[0], 1));

            var output = sortedCounts.take(100);
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._2() + ": " + tuple._1());
            }

            ctx.stop();
        }

    }

    private static JavaPairRDD<String, Integer> wordsByCount(JavaRDD<String> lines) {
        JavaRDD<String> words = lines
                .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
                .filter(s -> !s.isBlank());

        // map and use a regex to get rid of any non aphabetic characters
        var simpleWords = words.map(s -> s.replaceAll(NON_WORD_CHARS, ""));

        var lowerCaseWords = simpleWords.map(String::toLowerCase);

        var ones = lowerCaseWords.mapToPair(word -> new Tuple2<>(word, 1));

        var counts = ones.reduceByKey(Integer::sum);

        // swap word:count to count:word, sort by key, swap back
        var sortedCounts = counts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);
        return sortedCounts;
    }

    record WordCount(String word, int count) {}

}