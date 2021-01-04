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

package za.co.absa.atum.core

import org.apache.spark.sql.SparkSession

/**
  * The object coordinates access to control measurements state
  */
object AtumSdkS3 extends AtumSdkS3

class AtumSdkS3 extends Atum {

  private[atum] def controlFrameworkStateSdkS3: ControlFrameworkStateSdkS3 = state.asInstanceOf[ControlFrameworkStateSdkS3]

  override private[atum] def init(sparkSession: SparkSession): Unit = {
    preventDoubleInitialization(sparkSession)

    val s3State = new ControlFrameworkStateSdkS3(sparkSession)
    state = s3State // internal state assign

    sparkListener = new SparkEventListener(s3State)
    queryExecutionListener = new SparkQueryExecutionListenerSdkS3(s3State)

    sparkSession.sparkContext.addSparkListener(sparkListener)
    sparkSession.listenerManager.register(queryExecutionListener)

    val sessionConf = sparkSession.sessionState.conf
    sessionConf.setConfString(Constants.InitFlagKey, true.toString)
  }

  override private[atum] def dispose(sparkSession: SparkSession): Unit = {
    preventNotInitialized(sparkSession)

    if (state.havePendingCheckpoints) {
      AtumSdkS3.log.info(s"Saving control framework checkpoints")
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

}
