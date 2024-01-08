package org.example;

import lombok.SneakyThrows;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    @SneakyThrows
    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        List<String> rawLines = Files.readAllLines(Path.of(args[0]));

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

            var lines = ctx.parallelize(rawLines, 10);


            Stream<TopWordCounter.WordCount> wordCountStream = new TopWordCounter().topWords(lines);

            wordCountStream.forEach(System.out::println);

            ctx.stop();

        }



    }

}
