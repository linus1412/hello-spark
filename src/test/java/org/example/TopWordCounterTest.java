package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.TopWordCounter.WordCount;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TopWordCounterTest {

    SparkConf sparkConf = new SparkConf()
            .setAppName("JavaWordCount")
            .setMaster("local[*]");
    @Test
    void test() {

        List<String> lines = new ArrayList<>();
        lines.add("It was the best of times, it was the worst of times,");
        lines.add("it was the age of wisdom, it was the age of foolishness,");
        lines.add("it was the epoch of belief, it was the epoch of incredulity,");
        lines.add("it was the season of Light, it was the season of Darkness,");
        lines.add("it was the spring of hope, it was the winter of despair,");
        lines.add("we had everything before us, we had nothing before us,");
        lines.add("we were all going direct to Heaven, we were all going direct");
        lines.add("the other way--in short, the period was so far like the present");
        lines.add("period, that some of its noisiest authorities insisted on its");
        lines.add("being received, for good or for evil, in the superlative degree");
        lines.add("of comparison only.");

        try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

            Stream<WordCount> wordCountStream = new TopWordCounter().topWords(ctx.parallelize(lines, 10));

            assertThat(wordCountStream).contains(
                    new WordCount("it", 10),
                    new WordCount("only", 1),
                    new WordCount("comparison", 1),
                    new WordCount("darkness", 1),
                    new WordCount("way", 1),
                    new WordCount("in", 2)
            );


        }

    }


}