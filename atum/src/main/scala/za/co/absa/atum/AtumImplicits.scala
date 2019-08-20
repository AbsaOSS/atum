/*
 * Copyright 2018-2019 ABSA Group Limited
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

import scala.language.implicitConversions
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.atum.core.Atum.controlFrameworkState
import za.co.absa.atum.core.{Atum, Constants, SparkEventListener, SparkQueryExecutionListener}
import za.co.absa.atum.persistence._

/**
  * The object contains implicit methods for Control Framework
  * Minimalistic example of enabling control measurements tracking:
  *   {{{
  *   import za.co.absa.atum.Atum
  *   import za.co.absa.atum.AtumImplicits._
  *
  *   ...
  *
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
object AtumImplicits {
  type DefaultControlInfoStorer = ControlMeasuresStorerJsonFile
  type DefaultControlInfoLoader = ControlMeasuresLoaderJsonFile

  implicit def StringToPath(path: String): Path = new Path(path)

  /**
    * The class contains implicit methods for [[org.apache.spark.sql.SparkSession]].
    */
  implicit class SparkSessionWrapper(sparkSession: SparkSession) {

    /**
      * Enable control measurements tracking.
      * Input and output info file names will be inferred automatically based on data source and destination paths
      *
      */
    def enableControlMeasuresTracking(): SparkSession = {
      enableControlMeasuresTracking(None, None)
    }

    /**
      * Enable control measurements tracking.
      * Both input and output info file paths need to be provided
      *
      * Example info file path name: "data/input/wikidata.csv.info"
      *
      * @param sourceInfoFile      Pathname to a json-formatted info file containing control measurements
      * @param destinationInfoFile Pathname to save the control measurement results to
      */
    def enableControlMeasuresTracking(sourceInfoFile: String = "",
                                      destinationInfoFile: String = ""): SparkSession = {
      val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

      val loader = if (sourceInfoFile.isEmpty) None else Some(new DefaultControlInfoLoader(hadoopConfiguration, sourceInfoFile))
      val storer = if (destinationInfoFile.isEmpty) None else Some(new DefaultControlInfoStorer(hadoopConfiguration, destinationInfoFile))

      enableControlMeasuresTracking(loader, storer)
    }

    /**
      * Enable control measurements tracking.
      * This is a generic way to enable control measurements tracking enabling to provide a custom
      * control measurements loader and storer objects
      *
      * @param loader An object responsible for loading data source control measurements
      * @param storer An object responsible for storing the result control measurements
      */
    def enableControlMeasuresTracking(loader: Option[ControlMeasuresLoader],
                                      storer: Option[ControlMeasuresStorer]): SparkSession =
      sparkSession.synchronized {
        Atum.init(sparkSession)

        if (loader.nonEmpty) {
          Atum.setLoader(loader.get, sparkSession)
        }

        if (storer.nonEmpty) {
          Atum.setStorer(storer.get)
        }

        sparkSession
      }

    /**
      * Explicitly disable control measurements tracking.
      * After invoking this routine control measuress will not be tracked for the rest of the Spark Job
      *
      */
    def disableControlMeasuresTracking(): SparkSession =
      sparkSession.synchronized {
        Atum.dispose(sparkSession)

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
      Atum.setControlMeasuresInputFileName(fileName)
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
      Atum.setControlMeasuresOutputFileName(fileName)
      sparkSession
    }

    /**
      * The method sets workflow name for the current job
      *
      * @param workflowName Name of the checkpoint
      */
    def setControlMeasuresWorkflow(workflowName: String): SparkSession = {
      Atum.setWorkflowName(workflowName)
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
        Atum.setControlFrameworkError(sparkSession, jobStep, errorDescriptionTrunc, techDetailsTrunc)
      }
      sparkSession
    }

  }

  /**
    * The class contains implicit methods for [[org.apache.spark.sql.Dataset]].
    */
  implicit class DataSetWrapper(dataset: Dataset[Row]) {
    /**
      * The method creates a new checkpoint by calculating control measurements of the dataset
      * On first checkpoint Spark Session Key ControlFrameworkKeys.InfoFileVersionKey is updated
      * to the info file stored version
      *
      * @param name Name of the checkpoint
      */
    def setCheckpoint(name: String, persistInDatabase: Boolean = true): Dataset[Row] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      if (Atum.controlFrameworkState == null) {
        Atum.log.error("Attempt to create checkpoint before control measurements are loaded. Please ensure the source data file has corresponding _INFO file")
        dataset
      }
      else {
        Atum.controlFrameworkState.calculateCheckpoint(dataset, name, !persistInDatabase)
      }
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
      if (Atum.controlFrameworkState == null) {
        Atum.log.error("Attempt to create checkpoint before control measurements are loaded. Please ensure the source data file has corresponding _INFO file")
        None
      }
      else {
        Atum.controlFrameworkState.getRowCountOfLastCheckpoint
      }
    }

    /**
      * The method registers a column rename of a column that is used for control measurements
      *
      * @param oldName A job step name
      * @param newName An error description
      */
    def registerColumnRename(oldName: String, newName: String): Dataset[Row] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      controlFrameworkState.registerColumnRename(dataset, oldName, newName)
      dataset
    }

    /**
      * The method registers a column drop when it is no longer needed for the column to calculate control measurements
      *
      * @param columnName A column to be dropped from measurements
      */
    def registerColumnDrop(columnName: String): Dataset[Row] = {
      if (!(dataset.sparkSession.sessionState.conf contains Constants.InitFlagKey))
        throw new IllegalStateException("Control framework tracking is not initialized.")
      controlFrameworkState.registerColumnDrop(dataset, columnName)
      dataset
    }

    /**
      * The method fetches the initial control measurements and puts version from info file
      * to ControlFrameworkKeys.InfoFileVersionKey Spark Session Key
      *
      */
    def loadControlInfoFile(): Dataset[Row] = {
      Atum.controlFrameworkState.initializeControlInfo(dataset)
      dataset
    }

    /**
      * The method saves the info file to the specified destination path on HDFS
      *
      * @param outputPath A directory or a file name to save the info file to.
      */
    def writeInfoFile(outputPath: String): Dataset[Row] = {
      Atum.controlFrameworkState.storeCurrentInfoFile(outputPath)
      dataset
    }

  }

}
