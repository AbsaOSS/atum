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

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import za.co.absa.atum.persistence.{HadoopFsControlMeasuresStorer, S3ControlMeasuresStorer, S3KmsSettings}
import za.co.absa.atum.utils.ExecutionPlanUtils._
import za.co.absa.atum.utils.S3Utils

/**
 * The class is responsible for listening to DataSet save events and outputting corresponding control measurements.
 */
class SparkQueryExecutionListener(cf: ControlFrameworkState) extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (funcName == "save") {

      cf.accumulator.getStorer match {
        case Some(s3storer: S3ControlMeasuresStorer) =>
          Atum.log.debug(s"SparkQueryExecutionListener.onSuccess for S3ControlMeasuresStorer: writing to ${s3storer.outputLocation.s3String}")
          writeInfoFileForQueryForSdkS3(qe, s3storer.outputLocation.region, s3storer.kmsSettings)(s3storer.credentialsProvider)

        case Some(hadoopStorer: HadoopFsControlMeasuresStorer) =>
          Atum.log.debug(s"SparkQueryExecutionListener.onSuccess: writing to Hadoop FS")
          writeInfoFileForQuery(qe)(hadoopStorer.outputFs)

        case _ =>
          Atum.log.info("No usable storer is set, therefore no data will be written the automatically with DF-save to an _INFO file.")
      }

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
  private def writeInfoFileForQuery(qe: QueryExecution)(implicit outputFs: FileSystem): Unit = {
    val infoFilePath = inferOutputInfoFileName(qe, cf.outputInfoFileName)

    // Write _INFO file to the output directory
    infoFilePath.foreach(path => {
      Atum.log.info(s"Inferred _INFO Path = ${path.toUri.toString}")
      cf.storeCurrentInfoFile(path)
    })

    // Write _INFO file to a registered storer
    if (cf.accumulator.isStorerLoaded) {
      cf.accumulator.store()
    }
  }

  /** Write _INFO file with control measurements to the output directory based on the query plan */
  private def writeInfoFileForQueryForSdkS3(qe: QueryExecution, region: Region, kmsSettings: S3KmsSettings)(implicit credentialsProvider: AwsCredentialsProvider): Unit = {
    val infoFilePath = inferOutputInfoFileNameOnS3(qe, cf.outputInfoFileName)

    // Write _INFO file to the output directory
    infoFilePath.foreach(path => {

      import S3Utils.StringS3LocationExt
      val location = path.toS3LocationOrFail.withRegion(region)

      Atum.log.debug(s"Inferred _INFO Location = $location")
      cf.storeCurrentInfoFileOnSdkS3(location, kmsSettings)
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
