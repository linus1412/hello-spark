# Count and sort words in Spark

Using [SDKMAN](https://sdkman.io/) install the versions of Java, Spark and Maven as specified in the [pom.xml](pom.xml) file.

```bash
sdk env install
```

### Run the tests locally in the IDE (Optional)

You can TopWordCounterTest but you must set the VM argument

```--add-exports java.base/sun.nio.ch=ALL-UNNAMED```

### Compile the project using Maven.

```bash
mvn package
```

### Run the project using Spark.

```bash
spark-submit --class org.example.Main --master local ./target/hello-spark-1.0-SNAPSHOT.jar ./src/main/resources/book1.txt
```

