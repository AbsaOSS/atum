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

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import za.co.absa.atum.persistence.hdfs.ControlMeasuresHdfsStorerJsonFile
import za.co.absa.atum.utils.ExecutionPlanUtils.{inferOutputFileName, inferOutputInfoFileName}

/**
  * The class is responsible for listening to DataSet save events and outputting correcpoiding control measurements.
  */
class SparkQueryExecutionListener(cf: ControlFrameworkState) extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (funcName == "save") {
      writeInfoFileForQuery(qe)

      // Notify listeners
      cf.updateRunCheckpoints(saveInfoFile = true)
      cf.updateStatusSuccess()

      updateSplineRef(qe)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))

    cf.updateStatusFailure(qe.sparkSession.sparkContext.appName, funcName, exception.getMessage,
      sw.toString + "\r\n\r\n" + qe.optimizedPlan.toString)
  }

  /** Write _INFO file with control measurements to the output directory based on the query plan */
  private def writeInfoFileForQuery(qe: QueryExecution): Unit = {
    val infoFilePath = inferOutputInfoFileName(qe, cf.outputInfoFileName)

    // Write _INFO file to the output directory
    infoFilePath.foreach(path => {
      Atum.log.info(s"Infered _INFO Path = ${path.toUri.toString}")
      cf.storeCurrentInfoFile(path, qe.sparkSession.sparkContext.hadoopConfiguration)
    })

    // Write _INFO file to a registered storer
    if (cf.accumulator.isStorerLoaded) {
      cf.accumulator.store()
    }
  }

  /** Update Spline reference of the job and notify plugins */
  private def updateSplineRef(qe: QueryExecution): Unit = {
    val outputPath = inferOutputFileName(qe, qe.sparkSession.sparkContext.hadoopConfiguration)
    outputPath.foreach(path => {
      cf.updateSplineRef(path.toUri.toString)
      Atum.log.info(s"Infered Output Path = ${path.toUri.toString}")
    })
  }
}
