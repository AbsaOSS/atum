# Atum Spark Job Application Example

This is a set of Atum Apache Spark Applications that can be used as inspiration for creating other
Spark projects. It includes all dependencies in a 'fat' jar to run the job locally and on a cluster.

Here is the list of examples (all from `za.co.absa.atum.examples` space):

- `SampleMeasurements{1|2|3}` - Example apps using core Atum to show the Atum initialization, 
checkpoint setup and the resulting control measure handling (in the form of `_INFO` file)  
- `CreateInfoFileTool[CSV]` - Applications demonstrating the means of creating the initial `_INFO` for data (CSV or general)

## Usage

The example application is in `za.co.absa.atum.examples` package. The project contains build files for `Maven`.

## Maven

**To test-run this locally use**
```shell script
mvn test
```
(This will run the `SampleMeasurements{1|2|3}` jobs via `SampleMeasurementsHdfsRunnerSpec` as tests)

**To build an uber jar to run on cluster**
```shell script
mvn package -DskipTests=true
```

## Scala and Spark version switching
Same as Atum itself, the example project also supports switching to build with different Scala and Spark version:

Switching Scala version (2.11 or 2.12) can be done via
```shell script
mvn scala-cross-build:change-version -Pscala-2.11 # this is default
# or
mvn scala-cross-build:change-version -Pscala-2.12
```

Choosing a spark version to build, there are `spark-2.4` and `spark-3.1` profiles: 
```shell script
mvn clean install -Pspark-2.4 # this is default
mvn clean install -Pspark-3.1
``` 

## Running via spark-submit

After the project is packaged you can copy `target/2.11/atum-examples_2.11-0.0.1-SNAPSHOT.jar`
to an edge node of a cluster and use `spark-submit` to run the job. Here is an example when running on Yarn:

```shell script
spark-submit --master yarn --deploy-mode client --class za.co.absa.atum.examples.SampleMeasurements1 atum-examples_2.11-0.0.1-SNAPSHOT.jar
```

### Running Spark Applications in local mode from an IDE
If you try to run the example from an IDE you'll likely get the following exception: 
```Exception in thread "main" java.lang.NoClassDefFoundError: scala/Option```

This is because the jar is created with all Scala and Spark dependencies removed (using shade plugin). This is done so that the uber jar for `spark-submit` is not too big.

There are multiple options to deal with it, namely:
 - use the test runner class, for the SampleMeasurements, it is `SampleMeasurementsHdfsRunnerSpec` (provided dependencies will be loaded for tests)
 - use the  _Include dependencies with "Provided" scope_ option in Run Configuration in IDEA or equivalent in your IDE.
 - change the scope of `provided` dependencies to `compile` in the POM file and run Spark Applications as a normal JVM App.
