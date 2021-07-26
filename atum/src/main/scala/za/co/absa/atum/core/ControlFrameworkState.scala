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

package za.co.absa.atum.core

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import za.co.absa.atum.AtumImplicits.DefaultControlInfoLoader
import za.co.absa.atum.core.Atum.log
import za.co.absa.atum.core.ControlType.Count
import za.co.absa.atum.model.{RunError, RunState, _}
import za.co.absa.atum.persistence.hdfs.ControlMeasuresHdfsStorerJsonFile
import za.co.absa.atum.persistence.{ControlMeasuresLoader, ControlMeasuresStorer}
import za.co.absa.atum.plugins.EventListener
import za.co.absa.atum.utils.BuildProperties
import za.co.absa.atum.utils.ExecutionPlanUtils.inferInputInfoFilePath

import java.io.FileNotFoundException
import scala.util.control.NonFatal

/**
  * This class holds control measurement's state
  */
class ControlFrameworkState(sparkSession: SparkSession) {
  private[atum] val accumulator: Accumulator = new Accumulator()
  private[atum] var processor: MeasurementProcessor = _
  private[atum] var eventListeners: Seq[EventListener] = Seq.empty
  private[atum] var inputInfoFileName = Constants.DefaultInfoFileName
  private[atum] var outputInfoFileName = Constants.DefaultInfoFileName
  private[atum] var workflowName: String = ""
  private[atum] var havePendingCheckpoints: Boolean = false
  private[atum] var allowUnpersistOldDatasets: Boolean = false
  private[atum] var cacheStorageLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_AND_DISK)
  private[atum] var lastCachedDS: Dataset[Row] = _

  private[atum] def setLoader(loader: ControlMeasuresLoader, sparkSession: SparkSession): Unit = {
    accumulator.loadControlMeasurements(loader)
  }

  private[atum] def setStorer(storer: ControlMeasuresStorer): Unit = {
    accumulator.setStorer(storer)
  }

  private[atum] def setBuildProperties(buildProperties: BuildProperties): Unit = {
    accumulator.setBuildProperties(buildProperties)
  }

  private[atum] def setAllowUnpersistOldDatasets(allowUnpersist: Boolean): Unit = {
    allowUnpersistOldDatasets = allowUnpersist
  }

  private[atum] def setControlMeasuresInputFileName(fileName: String): Unit = {
    if (accumulator.isControlMeasuresLoaded) {
      throw new IllegalStateException("Cannot set input file name because control measurements are already loaded.")
    }
    if (fileName.isEmpty) {
      throw new IllegalStateException("Empty control framework file name is not allowed.")
    }
    inputInfoFileName = fileName
  }

  private[atum] def setControlMeasuresOutputFileName(fileName: String): Unit = {
    if (accumulator.isStorerLoaded) {
      throw new IllegalStateException("Cannot set output file name because control measurements output file is already set.")
    }
    if (fileName.isEmpty) {
      throw new IllegalStateException("Empty control framework file name is not allowed.")
    }
    outputInfoFileName = fileName
  }

  private[atum] def setRunUniqueId(uniqueId: String): Unit = {
    accumulator.setRunUniqueId(uniqueId)
  }

  private[atum] def setAdditionalInfo(kv: (String, String), replaceIfExists: Boolean): Unit = {
    accumulator.setAdditionalInfo(kv, replaceIfExists)
    havePendingCheckpoints = true
  }

  private[atum] def enableCaching(cacheStorageLevel: StorageLevel): Unit = {
    this.cacheStorageLevel = Option(cacheStorageLevel)
  }

  private[atum] def enableCaching(cacheStorageLevelStr: String): Unit = {
    cacheStorageLevel = Option(StorageLevel.fromString(cacheStorageLevelStr))
  }

  private[atum] def disableCaching(): Unit = {
    cacheStorageLevel = None
  }

  private[atum] def registerColumnRename(dataset: Dataset[Row], oldName: String, newName: String)(implicit inputFs: FileSystem): Unit = {
    initializeControlInfo(dataset)
    if (processor == null) {
      initializeProcessor(dataset.sparkSession)
    }
    processor.registerColumnRename(oldName, newName)
  }

  private[atum] def registerColumnDrop(dataset: Dataset[Row], columnName: String)(implicit inputFs: FileSystem): Unit = {
    initializeControlInfo(dataset)
    if (processor == null) {
      initializeProcessor(dataset.sparkSession)
    }
    processor.registerColumnDrop(columnName)
  }

  private[atum] def calculateCheckpoint(dataset: Dataset[Row], name: String, delayCheckpointPersistence: Boolean)
                                       (implicit inputFs: FileSystem): Dataset[Row] = {
    initializeControlInfo(dataset)
    if (processor == null) {
      initializeProcessor(dataset.sparkSession)
    }

    // Update pending checkpoints
    updateRunCheckpoints(saveInfoFile = false)

    // Caching the current dataset. Do not unpersist lastCachedDS yet as the current dataset may depend on it
    val cachedDS = cacheStorageLevel match {
      case Some(storageLevel) => dataset.persist(storageLevel)
      case None => dataset
    }

    val measurements = processor.measureDataset(cachedDS.as("checkpoint_" + name))
    val checkpoint = accumulator.addCheckpoint(name, workflowName, measurements)
    havePendingCheckpoints = true

    // Update new checkpoints
    if (!delayCheckpointPersistence) {
      updateRunCheckpoints(saveInfoFile = false)
    }


    if (allowUnpersistOldDatasets && cacheStorageLevel.isDefined) {
      // Explicitly mark the dataset of the previous checkpoint as no longer needed.
      // This way we are releasing resources for the future computations.
      if (lastCachedDS != null) {
        lastCachedDS.unpersist()
      }

      // Remember this dataset so we can unpersist it in the future
      lastCachedDS = cachedDS
    }

    cachedDS
  }

  private[atum] def getControlMeasure: ControlMeasure = {
    accumulator.getControlMeasure
  }

  private[atum] def getRowCountOfLastCheckpoint: Option[Long] = {
    if (!accumulator.isControlMeasuresLoaded) {
      None
    } else {
      val lastChheckpoint = accumulator.getCheckpoints.last
      var recordCount: Option[Long] = None
      lastChheckpoint.controls.foreach(measurement =>
        if (measurement.controlType == Count.value) {
          // controlValue can be Int, Long, BigInt. However we know that the number of records
          // is < 2^64, so it is ok cast it back to Long
          recordCount = Some(measurement.controlValue.toString.toLong)
        }
      )
      recordCount
    }
  }

  private[atum] def updateRunCheckpoints(saveInfoFile: Boolean): Unit = {
    if (saveInfoFile) {
      try {
        accumulator.store()
      } catch {
        case NonFatal(e) => log.warn(s"Unable to store control measurements via ${accumulator.getStorerInfo}. Error: ${e.getMessage}")
      }
    }
    if (havePendingCheckpoints) {
      for ( listener <- eventListeners ) {
        listener.onControlMeasurementsUpdated(accumulator.getControlMeasure)
      }
      havePendingCheckpoints = false
    }
  }

  private[atum] def updateRunStatus(rs: RunStatus): Unit = {
    for ( listener <- eventListeners ) {
      listener.onJobStatusChange(rs)
    }
  }

  private[atum] def updateStatusSuccess(): Unit = {
    val state = RunState.allSucceeded
    val rs: RunStatus = RunStatus(state, None)
    updateRunStatus(rs)
  }

  private[atum] def updateStatusinInProgress(): Unit = {
    val rs: RunStatus = RunStatus(RunState.running, None)
    updateRunStatus(rs)
  }

  private[atum] def updateStatusFailure(jobName: String, step: String, description: String, techDetails: String): Unit = {
    val runError = RunError(jobName, step, description, techDetails)
    val runStatus: RunStatus = RunStatus(RunState.failed, Some(runError))
    updateRunStatus(runStatus)
  }


  private[atum] def updateSplineRef(outputFileName: String): Unit = {
    for ( listener <- eventListeners ) {
      listener.onSaveOutput(sparkSession.sparkContext.applicationId, outputFileName)
    }
  }

  private[atum] def onApplicationEnd(): Unit = {
    for ( listener <- eventListeners ) {
      listener.onApplicationEnd()
    }
  }

  private[atum] def initializeControlInfo(dataset: Dataset[Row])(implicit inputFs: FileSystem): Unit = {
    if (!accumulator.isControlMeasuresLoaded) {
      val infoFilePath = inferInputInfoFilePath(dataset, inputInfoFileName)
      accumulator.loadControlMeasurements(new DefaultControlInfoLoader(infoFilePath))
    }
  }

  private[atum] def addEventListener(eventListener: EventListener): Unit = {
    eventListeners = eventListeners :+ eventListener

    // For a new listener if the control file is already loaded, trigger the onLoad event
    if (accumulator.isControlMeasuresLoaded) {
      if (processor == null) {
        initializeProcessor(sparkSession)
      } else {
        eventListener.onLoad(sparkSession.sparkContext.applicationId, inputInfoFileName, accumulator.getControlMeasure)
      }
    }
  }

  private[atum] def storeCurrentInfoFile(outputInfoFilePath: Path)(implicit outputFs: FileSystem): Unit = {
    val isDirectory: Boolean = try {
      outputFs.getFileStatus(outputInfoFilePath).isDirectory
    } catch {
      case _: FileNotFoundException => false
    }

    val outputFilePath = if (isDirectory) {
      new Path(outputInfoFilePath, outputInfoFileName)
    } else {
      outputInfoFilePath
    }

    val storer = ControlMeasuresHdfsStorerJsonFile(outputFilePath)
    storer.store(accumulator.getControlMeasure)
    Atum.log.info(s"Control measurements saved to ${outputFilePath.toUri.toString}")
  }

  private def initializeProcessor(sparkSession: SparkSession): Unit = {
    val checkpoints = accumulator.getCheckpoints

    if (checkpoints.isEmpty) {
      throw new IllegalStateException("Source control framework info file does not contain checkpoints.")
    }

    for ( listener <- eventListeners ) {
      listener.onLoad(sparkSession.sparkContext.applicationId, inputInfoFileName, accumulator.getControlMeasure)
    }

    val lastCheckpoint = checkpoints.reduceLeft((a, b) => if (a.order > b.order) a else b)
    processor = new MeasurementProcessor(lastCheckpoint.controls)

    val sessionConf = sparkSession.sessionState.conf
    sessionConf.setConfString(Constants.InfoFileVersionKey, accumulator.getControlMeasure.metadata.version.toString)
    sessionConf.setConfString(Constants.InfoFileDateKey, accumulator.getControlMeasure.metadata.informationDate)
    if (accumulator.getUniqueRunId.nonEmpty) {
      sessionConf.setConfString(Constants.RunUniqueIdKey, accumulator.getUniqueRunId.get)
    }
  }
}
