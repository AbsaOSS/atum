/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import za.co.absa.atum.core.{Atum, Constants}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence._
import za.co.absa.atum.persistence.hdfs.{ControlMeasuresHdfsLoaderJsonFile, ControlMeasuresHdfsStorerJsonFile}
import za.co.absa.atum.utils.{BuildProperties, DefaultBuildProperties, InfoFile}

import scala.language.implicitConversions

/**
  * The object contains implicit methods for Control Framework
  * Minimalistic example of enabling control measurements tracking:
  *   {{{
  *   import za.co.absa.atum.AtumImplicits._
  *
  *   ...
  *   // using basic Atum
  *   spark.enableControlFrameworkTracking(sourceInfoFile = "/source/info/file/path")
  *
  *   ...
  *
  *   dataSet.setCheckpoint("Checkpoint Name")
  *   }}}
  *
  * You can use enableControlFrameworkTracking() without parameters if the _INFO file
  * is in the path.
  *
  */
object AtumImplicits extends AtumImplicitsBase {
  implicit val atum: Atum = Atum
}

trait AtumImplicitsBase {
  type DefaultControlInfoStorer = ControlMeasuresHdfsStorerJsonFile
  type DefaultControlInfoLoader = ControlMeasuresHdfsLoaderJsonFile

  implicit class StringPathExt(path: String) {
    def toPath: Path = new Path(path)
  }

  /**
    * The class contains implicit methods for [[org.apache.spark.sql.SparkSession]].
    */
  implicit class AtumSparkSessionWrapper(sparkSession: SparkSession)(implicit atum: Atum) {

    /**
     * Enable control measurements tracking on HDFS | S3 (using Hadoop FS API).
     * Input and output info file names will be inferred automatically based on data source and destination paths if not provided
     *
     * Example info file path name: "data/input/wikidata.csv.info" or "s3://bucket1/folder1/wikidata.csv.info"
     *
     * @param sourceInfoFilePath      Pathname to a json-formatted info file containing control measurements
     * @param destinationInfoFilePath Pathname to save the control measurement results to
     */
    def enableControlMeasuresTracking(sourceInfoFilePath: Option[String],
                                      destinationInfoFilePath: Option[String]): SparkSession = {
      enableControlMeasuresTracking(sourceInfoFilePath, destinationInfoFilePath, DefaultBuildProperties)
    }

    /**
     * Enable control measurements tracking on HDFS | S3 (using Hadoop FS API).
     * Input and output info file names will be inferred automatically based on data source and destination paths if not provided
     *
     * Example info file path name: "data/input/wikidata.csv.info" or "s3://bucket1/folder1/wikidata.csv.info"
     *
     * @param sourceInfoFilePath      Pathname to a json-formatted info file containing control measurements
     * @param destinationInfoFilePath Pathname to save the control measurement results to
     * @param buildProperties         Build properties to be used while creating _INFO and adding checkpoints
     */
    def enableControlMeasuresTracking(sourceInfoFilePath: Option[String],
                                      destinationInfoFilePath: Option[String],
                                      buildProperties: BuildProperties): SparkSession = {
      implicit val hadoopConfiguration: Configuration = sparkSession.sparkContext.hadoopConfiguration

      val loader: Option[ControlMeasuresLoader] = sourceInfoFilePath.map(InfoFile(_).toDefaultControlInfoLoader)
      val storer: Option[ControlMeasuresStorer] = destinationInfoFilePath.map(InfoFile(_).toDefaultControlInfoStorer)

      enableControlMeasuresTrackingDirectly(loader, storer, buildProperties)
    }



    /**
     * Enable control measurements tracking with loader and/or storer directly.
     * This is a generic way to enable control measurements tracking enabling to provide a custom
     * control measurements loader and storer objects
     *
     * @param loader  An object responsible for loading data source control measurements
     * @param storer  An object responsible for storing the result control measurements
     *
     */
    private[atum] def enableControlMeasuresTrackingDirectly(loader: Option[ControlMeasuresLoader],
                                                            storer: Option[ControlMeasuresStorer],
                                                            buildProperties: BuildProperties = DefaultBuildProperties): SparkSession =
      sparkSession.synchronized {
        atum.init(sparkSession)

        if (loader.nonEmpty) {
          atum.setLoader(loader.get, sparkSession)
        }

        if (storer.nonEmpty) {
          atum.setStorer(storer.get)
        }

        atum.setBuildProperties(buildProperties)

        sparkSession
      }

    /**
      * Explicitly disable control measurements tracking.
      * After invoking this routine control measuress will not be tracked for the rest of the Spark Job
      *
      */
    def disableControlMeasuresTracking(): SparkSession =
      sparkSession.synchronized {
        atum.dispose(sparkSession)

        sparkSession
    }

    /**
      * Sets control measurements file name for the source and destination data set.
      * The file name should not contain path as it will be inferred from data source and destination.
      * Use this only if info file paths and not specified when calling enableControlFrameworkTracking()
      *
      * Example info file name: "_INFO"
      *
      * @param fileName A file name for control measurements info
      */
    def setControlMeasuresFileName(fileName: String): SparkSession = {
      setControlMeasuresInputFileName(fileName)
      setControlMeasuresOutputFileName(fileName)
      sparkSession
    }

    /**
      * Sets control measurements file name for the source data set.
      * The file name should not contain path as it will be inferred from data source.
      * Use this only if the input info file path and not specified when calling enableControlFrameworkTracking()
      *
      * Example info file name: "_INFO"
      *
      * @param fileName A file name for control measurements info
      */
    def setControlMeasuresInputFileName(fileName: String): SparkSession = {
      atum.setControlMeasuresInputFileName(fileName)
      sparkSession
    }

    /**
      * Sets control measurements file name for the destination data set.
      * The file name should not contain path as it will be inferred from data destination.
      * Use this only if the output info file path and not specified when calling enableControlFrameworkTracking()
      *
      * Example info file name: "_INFO"
      *
      * @param fileName A file name for control measurements info
      */
    def setControlMeasuresOutputFileName(fileName: String): SparkSession = {
      atum.setControlMeasuresOutputFileName(fileName)
      sparkSession
    }

    /**
      * The method sets workflow name for the current job
      *
      * @param workflowName Name of the checkpoint
      */
    def setControlMeasuresWorkflow(workflowName: String): SparkSession = {
      atum.setWorkflowName(workflowName)
      sparkSession
    }

    /**
      * Check if Control Framework is initialized
      *
      * @return true is Control Framework is initialized
      */
    def isControlMeasuresTrackingEnabled: Boolean = {
      sparkSession.sessionState.conf contains Constants.InitFlagKey
    }

    /**
      * The method notifies Menas of a job failure
      *
      * @param jobStep A job step name
      * @param errorDescription An error description
      * @param techDetails A technical details
      */
    def setControlMeasurementError(jobStep: String, errorDescription: String, techDetails: String): SparkSession = {
      val errorDescriptionTrunc = if (errorDescription.length > Constants.maxErrorMessageSize)
        errorDescription.substring(0, Constants.maxErrorMessageSize)
      else
        errorDescription
      val techDetailsTrunc = if (techDetails.length > Constants.maxErrorMessageSize)
        techDetails.substring(0, Constants.maxErrorMessageSize)
      else
        techDetails
      if (sparkSession.sessionState.conf contains Constants.InitFlagKey) {
        atum.setControlFrameworkError(sparkSession, jobStep, errorDescriptionTrunc, techDetailsTrunc)
      }
      sparkSession
    }

  }

  /**
    * The class contains implicit methods for [[org.apache.spark.sql.Dataset]].
    */
  implicit class DataSetWrapper(dataset: Dataset[Row])(implicit atum: Atum) {
    /**
      * The method creates a new checkpoint by calculating control measurements of the dataset
      * On first checkpoint Spark Session Key ControlFrameworkKeys.InfoFileVersionKey is updated
      * to the info file stored version
      *
      * @param name Name of the checkpoint
      */
    def setCheckpoint(name: String, persistInDatabase: Boolean = true)(implicit inputFs: FileSystem): Dataset[Row] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      if (atum.controlFrameworkState == null) {
        atum.log.error("Attempt to create checkpoint before control measurements are loaded. Please ensure the source data file has corresponding _INFO file")
        dataset
      }
      else {
        atum.controlFrameworkState.calculateCheckpoint(dataset, name, !persistInDatabase)
      }
    }

    /**
     * Sets an additional info in metadata of the _INFO file
     * @param keyAndValue - the pair of the _key_ and _value_ to add to the _INFO file, in a form of tuple
     * @param replaceIfExists - flag to indicate if the value should be replaced in the case the key already exists
     * @return - the original `DataFrame` the method was called upon
     */
    def setAdditionalInfo(key: String, value: String, replaceIfExists: Boolean = false): Dataset[Row] = {
      atum.preventNotInitialized()
      atum.controlFrameworkState.setAdditionalInfo(key, value, replaceIfExists)

      dataset
    }

    /**
     * The method returns ControlMeasure object from the Atum context
     * @return - ControlMeasure object containing all the checkpoints up to the current point
     */
    def getControlMeasure: ControlMeasure = {
      atum.preventNotInitialized()
      atum.controlFrameworkState.getControlMeasure
    }

    /**
      * The method returns the number of records in the dataframe calculated during the last checkpoint.
      * If record count is absent in the checkpoint measurements, None is returned.
      *
      * This is useful to optimize out an additional df.count() invocation in a Spark job with
      * enabled control measurements.
      */
    def lastCheckpointRowCount: Option[Long] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      if (atum.controlFrameworkState == null) {
        atum.log.error("Attempt to create checkpoint before control measurements are loaded. Please ensure the source data file has corresponding _INFO file")
        None
      }
      else {
        atum.controlFrameworkState.getRowCountOfLastCheckpoint
      }
    }

    /**
      * The method registers a column rename of a column that is used for control measurements
      *
      * @param oldName A job step name
      * @param newName An error description
      */
    def registerColumnRename(oldName: String, newName: String)(implicit inputFs: FileSystem): Dataset[Row] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      atum.controlFrameworkState.registerColumnRename(dataset, oldName, newName)
      dataset
    }

    /**
      * The method registers a column drop when it is no longer needed for the column to calculate control measurements
      *
      * @param columnName A column to be dropped from measurements
      */
    def registerColumnDrop(columnName: String)(implicit inputFs: FileSystem): Dataset[Row] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      atum.controlFrameworkState.registerColumnDrop(dataset, columnName)
      dataset
    }

    /**
      * The method fetches the initial control measurements and puts version from info file
      * to ControlFrameworkKeys.InfoFileVersionKey Spark Session Key
      *
      */
    def loadControlInfoFile(implicit inputFs: FileSystem): Dataset[Row] = {
      atum.controlFrameworkState.initializeControlInfo(dataset)
      dataset
    }

    /**
      * The method saves the info file to the specified destination path on HDFS
      *
      * @param outputPath A directory or a file name to save the info file to.
      */
    def writeInfoFile(outputPath: String)(implicit outputFs: FileSystem): Dataset[Row] = {
      atum.controlFrameworkState.storeCurrentInfoFile(outputPath.toPath)
      dataset
    }
  }
}
