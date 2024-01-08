package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TopWordCounter {

    private static final Pattern SPACE = Pattern.compile(" ");
    public static final String NON_WORD_CHARS = "[^a-zA-Z']";
    public static final String HYPHENS = "-+";

    public Stream<WordCount> topWords(JavaRDD<String> lines) {

        var sortedCounts = wordsByCount(lines);
        var output = sortedCounts.take(100);

        return output.stream()
                .map(t -> new WordCount(t._1(), t._2()));

    }

    public record WordCount(String word, int count) {}

    private JavaPairRDD<String, Integer> wordsByCount(JavaRDD<String> lines) {

        JavaRDD<String> words = lines
                .map(s -> s.replaceAll(HYPHENS, " "))
                .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
                .filter(s -> !s.isBlank());

        // map and use a regex to get rid of any non alphabetic characters
        var simpleWords = words.map(s -> s.replaceAll(NON_WORD_CHARS, ""));

        var lowerCaseWords = simpleWords.map(String::toLowerCase);

        var ones = lowerCaseWords.mapToPair(word -> new Tuple2<>(word, 1));

        var counts = ones.reduceByKey(Integer::sum);

        // swap word:count to count:word, sort by key (now count), swap back
        var sortedCounts = counts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);
        return sortedCounts;
    }

}