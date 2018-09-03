/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum.core

import org.apache.log4j.LogManager
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.atum.persistence.{ControlMeasuresLoader, ControlMeasuresStorer}
import za.co.absa.atum.plugins.EventListener

/**
  * The object coordinates access to control measurements state
  */
object Atum {
  private var state: ControlFrameworkState = _
  private var sparkListener: SparkListener = _
  private var queryExecutionListener: QueryExecutionListener = _
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
    * Allows Atum to automatically unpersist all cached datasets it creates before
    * calculating a checkpoint. This may lead to performance improvement,
    *
    * This feature is experimental and so it is disabled by default.
    */
  def setAllowUnpersistOldDatasets(allowUnpersist: Boolean): Unit = {
    preventNotInitialized()
    controlFrameworkState.setAllowUnpersistOldDatasets(allowUnpersist)
  }

  private[atum] def init(sparkSession: SparkSession): Unit = {
    preventDoubleInitialization(sparkSession)

    state = new ControlFrameworkState(sparkSession)

    sparkListener = new SparkEventListener(Atum.controlFrameworkState)
    queryExecutionListener = new SparkQueryExecutionListener(Atum.controlFrameworkState)

    sparkSession.sparkContext.addSparkListener(sparkListener)
    sparkSession.listenerManager.register(queryExecutionListener)

    val sessionConf = sparkSession.sessionState.conf
    sessionConf.setConfString(Constants.InitFlagKey, true.toString)
  }

  private[atum] def dispose(sparkSession: SparkSession): Unit = {
    preventNotInitialized(sparkSession)

    if (state.havePendingCheckpoints) {
      Atum.log.info(s"Saving control framework checkpoints")
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

  private[atum] def registerColumnRename(dataset: Dataset[Row], oldName: String, newName: String): Unit = {
    preventNotInitialized()
    controlFrameworkState.registerColumnRename(dataset, oldName, newName)
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

  private def preventDoubleInitialization(sparkSession: SparkSession): Unit = {
    val sessionConf = sparkSession.sessionState.conf
    if (sessionConf contains Constants.InitFlagKey)
      throw new IllegalStateException("Control framework tracking is already initialized.")
  }

}
