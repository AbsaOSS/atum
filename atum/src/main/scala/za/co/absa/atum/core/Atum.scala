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

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.{ControlMeasuresLoader, ControlMeasuresStorer}
import za.co.absa.atum.plugins.EventListener
import za.co.absa.atum.utils.BuildProperties

/**
  * The object coordinates access to control measurements state
  */
object Atum extends Atum

trait Atum {
  protected var state: ControlFrameworkState = _
  protected var sparkListener: SparkListener = _
  protected var queryExecutionListener: QueryExecutionListener = _
  private[atum] val log = LogManager.getLogger("Atum")

  /** There is one control framework state for now. */
  private[atum] def controlFrameworkState: ControlFrameworkState = state

  /**
    * Set's an unique id of a run stored in the _INFO file
    */
  def setRunUniqueId(uniqueId: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.setRunUniqueId(uniqueId)
  }

  /**
    * Sets an additional info in metadata of the _INFO file
    */
  def setAdditionalInfo(kv: (String, String), replaceIfExists: Boolean = false): Unit = {
    preventNotInitialized()
    controlFrameworkState.setAdditionalInfo(kv, replaceIfExists)
  }


  /**
    * Turns on caching that happens every time a checkpoint is generated (default behavior).
    *
    * Atum uses the default storage level for Spark 2.4 and later. But you can
    * specify a different Spark storage level to use for caching.
    * (See the definition of [[org.apache.spark.storage.StorageLevel]] for the details).
    *
    * It can be one of the following:
    * NONE, DISK_ONLY, DISK_ONLY_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER,
    * MEMORY_ONLY_SER_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER,
    * MEMORY_AND_DISK_SER_2, MEMORY_AND_DISK_SER_2, OFF_HEAP.
    *
    * @param cacheStorageLevel A caching storage level.
    */
  def enableCaching(cacheStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = {
    preventNotInitialized()
    controlFrameworkState.enableCaching(cacheStorageLevel)
    logCachingStorageLevel()
  }

  /**
    * Same as `enableCaching()` with `cacheStorageLevel` being a mandatory parameter.
    */
  def setCachingStorageLevel(cacheStorageLevel: StorageLevel): Unit = {
    preventNotInitialized()
    controlFrameworkState.enableCaching(cacheStorageLevel)
    logCachingStorageLevel()
  }

  /**
    * Same as `enableCaching()` with `cacheStorageLevel` being a mandatory parameter.
    * The storage level is passed as a `String` here for forward compatibility.
    */
  def setCachingStorageLevel(cacheStorageLevelStr: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.enableCaching(cacheStorageLevelStr)
    logCachingStorageLevel()
  }

  /**
    * Turns off caching that happens every time a checkpoint is generated.
    */
  def disableCaching(): Unit = {
    preventNotInitialized()
    log.info(s"Caching on checkpoints: DISABLED")
    controlFrameworkState.disableCaching()
  }

  /**
    * Returns the current caching storage level.
    */
  def cachingStorageLevel: StorageLevel = {
    preventNotInitialized()
    controlFrameworkState.cacheStorageLevel match {
      case Some(storageLevel) => storageLevel
      case None => StorageLevel.fromString("NONE")
    }
  }

  /**
    * Allows Atum to automatically unpersist all cached datasets it creates before
    * calculating a checkpoint. This may lead to performance improvement,
    *
    * This feature is experimental and so it is disabled by default.
    */
  def setAllowUnpersistOldDatasets(allowUnpersist: Boolean): Unit = {
    preventNotInitialized()
    controlFrameworkState.setAllowUnpersistOldDatasets(allowUnpersist)
  }


  /**
    * Returns the current control measures object containing all the checkpoints up to the current point.
    */
  def getControlMeasure: ControlMeasure = {
    preventNotInitialized()
    controlFrameworkState.getControlMeasure
  }

  private[atum] def init(sparkSession: SparkSession): Unit = {
    preventDoubleInitialization(sparkSession)

    state = new ControlFrameworkState(sparkSession)

    sparkListener = new SparkEventListener(controlFrameworkState)
    queryExecutionListener = new SparkQueryExecutionListener(controlFrameworkState)

    sparkSession.sparkContext.addSparkListener(sparkListener)
    sparkSession.listenerManager.register(queryExecutionListener)

    val sessionConf = sparkSession.sessionState.conf
    sessionConf.setConfString(Constants.InitFlagKey, true.toString)
  }

  private[atum] def dispose(sparkSession: SparkSession): Unit = {
    preventNotInitialized(sparkSession)

    if (state.havePendingCheckpoints) {
      log.info(s"Saving control framework checkpoints")
      state.updateRunCheckpoints(saveInfoFile = true)
    }

    sparkSession.sparkContext.removeSparkListener(sparkListener)
    sparkSession.listenerManager.unregister(queryExecutionListener)

    sparkListener.onApplicationEnd(null)

    val sessionConf = sparkSession.sessionState.conf
    sessionConf.unsetConf(Constants.InitFlagKey)

    sparkListener = null
    queryExecutionListener = null
    state = null
  }

  private[atum] def setLoader(loader: ControlMeasuresLoader, sparkSession: SparkSession): Unit = {
    preventNotInitialized()
    controlFrameworkState.setLoader(loader, sparkSession)
  }

  private[atum] def setStorer(storer: ControlMeasuresStorer): Unit = {
    preventNotInitialized()
    controlFrameworkState.setStorer(storer)
  }

  private[atum] def setBuildProperties(buildProperties: BuildProperties): Unit = {
    preventNotInitialized()
    controlFrameworkState.setBuildProperties(buildProperties)
  }

  private[atum] def setControlMeasuresInputFileName(fileName: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.setControlMeasuresInputFileName(fileName)
  }

  private[atum] def setControlMeasuresOutputFileName(fileName: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.setControlMeasuresOutputFileName(fileName)
  }

  private[atum] def setWorkflowName(workflowName: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.workflowName = workflowName
  }

  private[atum] def registerColumnRename(dataset: Dataset[Row], oldName: String, newName: String)(implicit inputFs: FileSystem): Unit = {
    preventNotInitialized()
    controlFrameworkState.registerColumnRename(dataset, oldName, newName)
  }

  private[atum] def registerColumnDrop(dataset: Dataset[Row], columnName: String)(implicit inputFs: FileSystem): Unit = {
    preventNotInitialized()
    controlFrameworkState.registerColumnDrop(dataset, columnName)
  }

  private[atum] def setControlFrameworkError(sparkSession: SparkSession, jobStep: String, errorDescription: String, techDetails: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.updateStatusFailure(sparkSession.sparkContext.appName, jobStep, errorDescription, techDetails)
  }

  private[atum] def addEventListener(eventListener: EventListener): Unit = {
    controlFrameworkState.addEventListener(eventListener)
  }

  private[atum] def preventNotInitialized(): Unit = {
    if (state == null)
      throw new IllegalStateException("Control framework tracking is not initialized.")
  }

  private[atum] def preventNotInitialized(sparkSession: SparkSession): Unit = {
    preventNotInitialized()
    if (!(sparkSession.sessionState.conf contains Constants.InitFlagKey))
      throw new IllegalStateException("Control framework tracking is not initialized.")
  }

  protected def preventDoubleInitialization(sparkSession: SparkSession): Unit = {
    val sessionConf = sparkSession.sessionState.conf
    if (sessionConf contains Constants.InitFlagKey)
      throw new IllegalStateException("Control framework tracking is already initialized.")
  }

  private def logCachingStorageLevel(): Unit = {
    controlFrameworkState.cacheStorageLevel match {
      case Some(storageLevel) =>
        log.info(s"Caching on checkpoints: ENABLED ${storageLevel.description}.")
      case None =>
        log.info(s"Caching on checkpoints: DISABLED")
    }
  }

}
