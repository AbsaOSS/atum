# About Atum

Atum is a data completeness and accuracy library for Apache Spark.

One of the challenges regulated industries face is the requirement to track and prove that their systems preserve 
the accuracy and completeness of data. In an attempt to solve this data processing problem in Apache Spark applications, 
we propose the approach implemented in this library. 

The purpose of Atum is to add the ability to specify "checkpoints" in Spark applications. These checkpoints are used 
to designate when and what metrics are calculated to ensure that critical input values have not been modified as well 
as allow for quick and efficient representation of the completeness of a dataset. Additional metrics can also be defined 
at any checkpoint.

Atum adopts a standard JSON message schema for capturing checkpoint data, thus can be extended upstream or downstream,
providing flexibility across other computation engines and programming languages.

The library provides a concise and dynamic way to track completeness and accuracy of data produced from source through
a pipeline of Spark applications. All metrics are calculated at a DataFrame level using various aggregation functions 
and are stored as metadata together with the data between Spark applications in pipeline. Comparing control metrics 
for various checkpoints is not only helpful for complying with strict regulatory frameworks, but also helps during 
development and debugging.
 

## Motivation

Big Data strategy for a company usually includes data gathering and ingestion processes.
That is the definition of how data from different systems operating inside a company
are gathered and stored for further analysis and reporting. An ingestion processes can involve
various transformations like:
* Converting between data formats (XML, CSV, etc.)
* Data type casting, for example converting XML strings to numeric values
* Joining reference tables. For example this can include enriching existing
  data with additional information available through dictionary mappings.
This constitutes a common ETL (Extract, Transform and Load) process.   

During such transformations, sometimes data can get corrupted (e.g. during casting), records can
get added or lost. For instance, *outer joining* a table holding duplicate keys can result in records explosion.
And *inner joining* a table which has no matching keys for some records will result in loss of records.

In regulated industries it is crucial to ensure data integrity and accuracy. For instance, in the banking industry
the BCBS set of regulations requires analysis and reporting to be based on data accuracy and integrity principles.
Thus it is critical at the ingestion stage to preserve the accuracy and integrity of the data gathered from a
source system.    

The purpose of Atum is to provide means of ensuring no critical fields have been modified during
the processing and no records are added or lost. To do this the library provides an ability
to calculate *hash sums* of explicitly specified columns. We call the set of hash sums at a given time
a *checkpoint* and each hash sum we call a *control measurement*. Checkpoints can be calculated anytime
between Spark transformations and actions.

We assume the data for ETL are processed in a sesies of batch jobs. Let's call each data set for a given batch
job a *batch*. All checkpoints are calculated for a specific batch.  

## Features

Atum provides means for defining, calculating and storing of checkpoints for batches. It does so by keeping additional
metadata in what we call an *info file*. An info file is a file usually named '_INFO' which usually resides in the same
directory as the data of a specific batch. Each time a data progresses through a pipeline of ETL transformations the info
file is extended by additional checkpoints made between processing steps.

A checkpoint can be generated between any Spark transformations and actions by invoking the '.setCheckpoint()' method
on a data frame. Checkpoints are generated eagerly so invoking '.setCheckpoint()' triggers several Spark actions
depending on the number of measurements required. When output data are saved, Atum saves an info file along with them.
It contains all checkpoints from the input data plus the new checkpoints generated in the spark job.

Features:
*   Create checkpoints
*   Store sequences of checkpoints in info files alongside data.
*   Automatically infer info file names by analyzing logical execution plans.
*   Provide an initial info file content generator routine (**ControlMeasureBuilder.forDf(...).build.asJson**) for Spark dataframes 
*   Field rename is supported, but if a field is part of a control measurements calculation the renaming should be
    explicitly stated using the **spark.registerColumnRename()** method.
*   Plugin support
 
Plugins are implemented as event listeners. To create a plugin you need to extend the
**'za.co.absa.atum.plugins.EventListener'** trait and register the plugin by passing it as an argument to
'PluginManager.loadPlugin()'. After this Atum will send plugin events to the event listener. This is useful 
for implementing generic notifications to be sent to a dashboard on checkpoint events.    

Limitations:
*   If there are several data sources involved in a computation only one of them should have an _INFO file.
If that is not the case the location of the _INFO file needs to be specified explicitly to resolve the ambiguity.
*   Several batch blocks, each having an info file, cannot be processed together. Batch blocks should be processed
independently.
 

## Usage

### Coordinate for Maven POM dependency
For project using Scala 2.11
```xml
<dependency>
    <groupId>za.co.absa</groupId>
    <artifactId>atum_2.11</artifactId>
    <version>3.5.0</version>
</dependency>
```
For project using Scala 2.12
```xml
<dependency>
    <groupId>za.co.absa</groupId>
    <artifactId>atum_2.12</artifactId>
    <version>3.5.0</version>
</dependency>
```

### Initial info file generation example

Atum provides helper methods for initial creation of info files from a Spark dataframe. It can be used as is or can
serve as a reference implementation for calculating control measurements.

#### Obtaining a ControlMeasure
The builder instance obtained by `ControlMeasureBuilder.forDf()` accepts some metadata via optional setters. 
In addition it accepts the list of fields for which control measurements should be generated. Depending on the data type 
of a field the method will generate a different control measurement. For numeric types it will generate 
**controlType.absAggregatedTotal**, e.g. **SUM(ABS(X))**. For non-numeric types it will generate 
**controlType.HashCrc32** e.g. **SUM(CRC32(x))**. Non-primitive data types are not supported.   


```scala
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.controlmeasure.ControlMeasureBuilder

val dataSourceName = "Source Application"
val inputPath = "/path/to/source"
val batchDate = "15-10-2017"
val batchVersion = 1

val spark = SparkSession.builder()
  .appName("An info file creation job")
  .getOrCreate()

val df: DataFrame = spark
  .read
  .format("csv").option("header", "true") // adjust to your data source format
  .load(inputPath)
val aggregateColumns = List("employeeId", "address", "dealId") // these columns must exist in the `df`

// builder-like fluent API to construct a ControlMeasureBuilder and yield the `controlMeasure` with `build`
val controlMeasure: ControlMeasure =
  ControlMeasureBuilder.forDf(df)
    .withAggregateColumns(aggregateColumns)
    .withInputPath(inputPath)
    .withSourceApplication(dataSourceName)
    .withReportDate(batchDate)
    .withReportVersion(batchVersion)
    .build

// convert to JSON using .asJson | asJsonPretty
println("Generated control measure is: " + controlMeasure.asJson)
```

#### Writing an _INFO file with the ControlMeasure to HDFS
```scala
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils

// assuming `spark`, `controlMeasure`, and `inputPath` from the previous example block
implicit val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
ControlMeasureUtils.writeControlMeasureInfoFileToHadoopFs(controlMeasure, new Path(inputPath))
```

#### Writing an _INFO file with the ControlMeasure to S3 (using Hadoop FS)
```scala
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils

// assuming `spark`, `controlMeasure`, and `inputPath` from the previous example block
val s3Uri = new URI("s3://my-awesome-bucket123") // s3://<bucket> (or s3a://)
val s3Path = new Path(s"/$inputPath") // /<text-file-object-path>

implicit val s3fs = FileSystem.get(s3Uri, spark.sparkContext.hadoopConfiguration)
ControlMeasureUtils.writeControlMeasureInfoFileToHadoopFs(controlMeasure, s3Path)

```

### An ETL job example 

For the full example please see **SampleMeasurements1** and **SampleMeasurements2** objects from *atum.examples* project.
It uses made up Wikipedia data for computations. The source data has an info file containing the initial checkpoints,
presumably generated by previous processing.

The examples are made so they can be run on a user's instance of Spark cluster. Spark and Scala dependencies have 'provided'
scope. To run them locally please use **SampleMeasurements1Runner** and **SampleMeasurements2Runner** test suites. When
running unit tests Maven loads all provided dependencies so all Scala and Spark libraries needed to run the jobs are
available when running unit tests.


```scala
import org.apache.spark.sql.SparkSession
import za.co.absa.atum.AtumImplicits._ // using basic Atum without extensions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object ExampleSparkJob {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Example Spark Job")
      .getOrCreate()

    import spark.implicits._

    // implicit FS is needed for enableControlMeasuresTracking, setCheckpoint calls, e.g. standard HDFS here:
    implicit val localHdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(sourceInfoFilePath = Some("data/input/_INFO"), destinationInfoFilePath = None)
      .setControlMeasuresWorkflow("Example processing")

    // Reading data from a CSV file and creating a checkpoint 
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/mydata.csv")
      .as("source")
      .setCheckpoint("Computations Started") // First checkpoint

    // A business logic of a spark job ...

    // The df.setCheckpoint() routine can be used as many time as needed.
    df.setCheckpoint("Computations Finished") // Second checkpoint
      .parquet("data/output/my_results")
  }
}
```

In this example the data is read from 'data/input/mydata.csv' file. This data file has a precomputed set of checkpoints
in 'data/input/_INFO'. Two checkpoints are created. Any business logic can be inserted between reading the source data
and saving it to Parquet format.  

### Storing Measurements in AWS S3

#### AWS S3 via Hadoop FS API
Since version 3.1.0, persistence support for AWS S3 via Hadoop FS API is available. The usage is the same as with 
regular HDFS with the exception of providing a different file system, e.g.:
```scala
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import za.co.absa.atum.AtumImplicits._ // using basic Atum without extensions

val spark = SparkSession
      .builder()
      .appName("Example Spark Job")
      .getOrCreate()

val s3Uri = new URI("s3://my-awesome-bucket")
implicit  val fs = FileSystem.get(s3Uri, spark.sparkContext.hadoopConfiguration)

```
The rest of the usage is the same in the example listed above.

#### AWS S3 via AWS SDK for S3
Starting with version 3.3.0, there is also persistence support for AWS S3 via AWS SDK S3 via an optional dependency:
```xml
<dependency>
    <groupId>za.co.absa</groupId>
    <artifactId>atum-s3-sdk-extension_2.11</artifactId> <!-- or 2.12 -->
    <version>${project.version}</version> <!-- e.g. 3.3.0 -->
</dependency>
```

The following example demonstrates the setup:
```scala
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider, ProfileCredentialsProvider}
import za.co.absa.atum.persistence.{S3KmsSettings, S3Location}
import za.co.absa.atum.AtumImplicitsSdkS3._ // using extended Atum

object S3Example {
  def main(args: Array[String]) {
    val spark = SparkSession
          .builder()
          .appName("Example S3 Atum init showcase")
          .getOrCreate()

    // Here we are using default credentials provider that relies on its default credentials provider chain to obtain the credentials
    // (e.g. running in EMR/EC2 with correct role assigned)
    implicit val defaultCredentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.create()
    // Alternatively, one could pass specific credentials provider. An example of using local profile named "saml" can be:
    // implicit val samlCredentialsProvider = ProfileCredentialsProvider.create("saml")
    
    val sourceS3Location: S3Location = S3Location("my-bucket123", "atum/input/my_amazing_measures.csv.info")

    val kmsKeyId: String = "arn:aws:kms:eu-west-1:123456789012:key/12345678-90ab-cdef-1234-567890abcdef" // just example 
    val destinationS3Config: (S3Location, S3KmsSettings) = (
      S3Location("my-bucket123", "atum/output/my_amazing_measures2.csv.info"),
      S3KmsSettings(kmsKeyId)
    )

    import spark.implicits._

    // Initializing library to hook up to Apache Spark with S3 persistence
    spark.enableControlMeasuresTrackingForS3(
      sourceS3Location = Some(sourceS3Location),
      destinationS3Config = Some(destinationS3Config)
    ).setControlMeasuresWorkflow("A job with measurements saved to S3")
  }
}

```
The rest of the processing logic and programmatic approach to the library remains unchanged.


### Standalone model usage
In cases you only want to work with Atum's model (`ControlMeasure`-related case classes and `S3Location`), you may find
Atum's model artifact sufficient as your dependency.

First, if not provided by Spark or other library, you will need to provide json4s dependencies. This project is tested 
with `3.5.3` and `3.7.0-M15`.
```xml
<dependency>
    <groupId>org.json4s</groupId>
    <artifactId>json4s-core_2.11</artifactId> <!-- or 2.12 -->
    <version>${json4s.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.json4s</groupId>
    <artifactId>json4s-jackson_2.11</artifactId> <!-- or 2.12 -->
    <version>${json4s.version}</version>
    <scope>provided</scope>
</dependency>
```

Then, just include the model library
```xml
<dependency>
    <groupId>za.co.absa</groupId>
    <artifactId>atum-model_2.11</artifactId> <!-- or 2.12 -->
    <version>3.5.0</version>
</dependency>
```

The model module also offers basic JSON (de)serialization functionality, such as:
```scala
import za.co.absa.atum.model._
import za.co.absa.atum.utils.SerializationUtils

val measureObject1: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](myJsonStringWithAControlMeasure)
val jsonString: String = SerializationUtils.asJson(measureObject1)
val prettyfiedJsonString: String = SerializationUtils.asJsonPretty(measureObject1)
```

## Atum library routines

The summary of common control framework routines you can use as Spark and Dataframe implicits are as follows:

| Routine        | Description          | Example usage  |
| -------------- |:-------------------- |:---------------|
| enableControlMeasuresTracking(sourceInfoFilePath: *Option[String]*, destinationInfoFilePath: *Option[String]*) | Enable control measurements tracking. Source and destination info file paths can be omitted. If omitted (`None`), they will be automatically inferred from the input/output data sources. | spark.enableControlMeasurementsTracking() |
| enableControlMeasuresTrackingForSdkS3(sourceS3Location: *Option[S3Location]*, destinationS3Config: *Option[(S3Location, S3KmsSettings)]*) | Enable control measurements tracking in S3. Source and destination parameters can be omitted. If omitted, the loading/storing part will not be used | spark.enableControlMeasuresTrackingForS3(optionalSourceS3Location, optionalDestinationS3Config) |
| isControlMeasuresTrackingEnabled: *Boolean* | Returns true if control measurements tracking is enabled. |  if (spark.isControlMeasuresTrackingEnabled) {/*do something*/} |
| disableControlMeasuresTracking() | Explicitly turn off control measurements tracking. | spark.disableControlMeasurementsTracking() |
| setCheckpoint(name: *String*) | Calculates the control measurements and appends a new checkpoint. | df.setCheckpoint("Conformance Started") |
| writeInfoFile(outputFileName: *String*) | Write only an info file to a given HDFS location (could be a directory of a file). | df.writeInfoFile("/project/test/_INFO") |
| registerColumnRename(oldName: *String*, newName: *String*) | Register that a column which is part of control measurements is renamed. | df.registerColumnRename("tradeNumber", "tradeId") |
| registerColumnDrop(columnName: *String*) | Register that a column which is part of control measurements is dropped. | df.registerColumnDrop("personId") |
| setControlMeasuresFileName(fileName: *String*) | Use a specific name for info files instead of deafult '_INFO'. | spark.setControlMeasuresFileName("_EXAMPLE_INFO") |
| setControlMeasuresWorkflow(workflowName: *String*) | Sets workflow name for the set of checkpoints that will follow. | spark.setControlMeasuresWorkflow("Conformance") |
| setControlMeasurementError(jobStep: *String*, errorDescription: *String*, techDetails: *String*) | Sets up an error message that can be used by plugins (e.g. Menas) to track the status of the job. | setControlMeasurementError("Conformance", "Validation error", stackTrace) |
| setAllowUnpersistOldDatasets(allowUnpersist: *Boolean)* | Turns on a performance optimization that unpersists old checkpoints after new onces are materialized. | Atum.setAllowUnpersistOldDatasets(true) |
| enableCaching(cacheStorageLevel: *StorageLevel*) | Turns on caching that happens every time a checkpoint is generated (default behavior). A specific storage level can be set as well (see `setCachingStorageLevel()`) | enableCaching() |
| disableCaching() | Turns off caching that happens every time a checkpoint is generated. | disableCaching() |
| setCachingStorageLevel(cacheStorageLevel: *StorageLevel*) | Specifies a Spark storage level to use for caching. Can be one of following: `NONE`, `DISK_ONLY`, `DISK_ONLY_2`, `MEMORY_ONLY`, `MEMORY_ONLY_2`, `MEMORY_ONLY_SER`, `MEMORY_ONLY_SER_2`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_2`, `MEMORY_AND_DISK_SER`, `MEMORY_AND_DISK_SER_2`, `MEMORY_AND_DISK_SER_2`, `OFF_HEAP`. | setCachingStorageLevel(StorageLevel.MEMORY_AND_DISK) |

## Control measurement types

The control measurement of a column is a hash sum. It can be calculated differently depending on the column's data type and
on business requirements. This table represents all currently supported measurement types:

| Type                           | Description                                           |
| ------------------------------ |:----------------------------------------------------- |
| controlType.Count              | Calculates the number of rows in the dataset          |
| controlType.distinctCount      | Calculates DISTINCT(COUNT(()) of the specified column |
| controlType.aggregatedTotal    | Calculates SUM() of the specified column              |
| controlType.absAggregatedTotal | Calculates SUM(ABS()) of the specified column         |
| controlType.HashCrc32          | Calculates SUM(CRC32()) of the specified column       |
