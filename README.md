# Count and sort words in Spark

Using [SDKMAN](https://sdkman.io/) install the versions of Java, Spark and Maven as specified in the [.sdkmanrc(.sdkmanrc) file.

```bash
sdk env install
```

### Step 0: Run the tests locally in the IDE (Optional)

You can TopWordCounterTest but you must set the VM argument

```--add-exports java.base/sun.nio.ch=ALL-UNNAMED```

### Step 1: Compile and package the project using Maven.

```bash
mvn package
```

### Step 2: Run the project in Spark.

```bash
spark-submit --class org.example.Main --master local ./target/hello-spark-1.0-SNAPSHOT.jar ./src/main/resources/book1.txt
```

