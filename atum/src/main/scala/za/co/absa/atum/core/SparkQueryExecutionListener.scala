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

import java.io.{PrintWriter, StringWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.util.QueryExecutionListener
import za.co.absa.atum.utils.{ExecutionPlanUtils, InfoFile}

/**
 * The class is responsible for listening to DataSet save events and outputting corresponding control measurements.
 */
class SparkQueryExecutionListener(cf: ControlFrameworkState) extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    def writeInfoFileCommon(qe: QueryExecution): Unit = {
      Atum.log.debug(s"SparkQueryExecutionListener.onSuccess: writing to Hadoop FS")
      writeInfoFileForQuery(qe)

      // Notify listeners
      cf.updateRunCheckpoints(saveInfoFile = true)
      cf.updateStatusSuccess()
      updateSplineRef(qe)
    }

    (funcName, qe.analyzed) match {
      case ("save", _) => writeInfoFileCommon(qe) // < spark 3.1.x
      case ("command", saveCommand)
        if saveCommand.isInstanceOf[SaveIntoDataSourceCommand] || saveCommand.isInstanceOf[InsertIntoHadoopFsRelationCommand]  // spark 3.2+
      => writeInfoFileCommon(qe)
      case _ =>
      // explanation: https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-31-to-32
      // "In Spark 3.2, the query executions triggered by DataFrameWriter are always named command when being sent
      // to QueryExecutionListener. In Spark 3.1 and earlier, the name is one of save, insertInto, saveAsTable."
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))

    cf.updateStatusFailure(qe.sparkSession.sparkContext.appName, funcName, exception.getMessage,
      sw.toString + "\r\n\r\n" + qe.optimizedPlan.toString)
  }

  /** Write _INFO file with control measurements to the output directory based on the query plan */
  private[core] def writeInfoFileForQuery(qe: QueryExecution)(): Unit = {
    val infoFileDir: Option[String] = ExecutionPlanUtils.inferOutputInfoFileDir(qe)

    implicit val hadoopConf: Configuration = qe.sparkSession.sparkContext.hadoopConfiguration
    val fsWithDir: Option[(FileSystem, Path)] = infoFileDir
      .map(InfoFile(_).toFsPath) // path + FS based on HDFS or S3 over hadoopFS

    // Write _INFO file to the output directory
    fsWithDir.foreach { case (fs, dir) =>
      val path = new Path(dir, cf.outputInfoFileName)

      Atum.log.info(s"Inferred _INFO Path = ${path.toUri.toString}")
      cf.storeCurrentInfoFile(path)(fs)
    }

    // Write _INFO file to a registered storer
    if (cf.accumulator.isStorerLoaded) {
      cf.accumulator.store()
    }
  }

  /** Update Spline reference of the job and notify plugins */
  protected def updateSplineRef(qe: QueryExecution): Unit = {
    val outputPath = ExecutionPlanUtils.inferOutputFileName(qe, qe.sparkSession.sparkContext.hadoopConfiguration)
    outputPath.foreach(path => {
      cf.updateSplineRef(path.toUri.toString)
      Atum.log.info(s"Inferred Output Path = ${path.toUri.toString}")
    })
  }
}
