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

import org.apache.spark.sql.execution.QueryExecution
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import za.co.absa.atum.persistence.S3ControlMeasuresStorer
import za.co.absa.atum.persistence.s3.S3KmsSettings
import za.co.absa.atum.utils.ExecutionPlanUtils

/**
 * The class is responsible for listening to DataSet save events and outputting corresponding control measurements.
 */
class SparkQueryExecutionListenerSdkS3(cf: ControlFrameworkStateSdkS3) extends SparkQueryExecutionListener(cf) {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (funcName == "save") {

      // adding s3 processing
      cf.accumulator.getStorer match {
        case Some(s3storer: S3ControlMeasuresStorer) =>
          AtumSdkS3.log.debug(s"SparkQueryExecutionListener.onSuccess for S3ControlMeasuresStorer: writing to ${s3storer.outputLocation.s3String}")
          writeInfoFileForQueryForSdkS3(qe, s3storer.outputLocation.region, s3storer.kmsSettings)(s3storer.credentialsProvider)

          // Notify listeners
          cf.updateRunCheckpoints(saveInfoFile = true)
          cf.updateStatusSuccess()
          updateSplineRef(qe)

        case _ =>
          // regular SQE processing
          super.onSuccess(funcName, qe, durationNs)
      }
    }
  }

   /** Write _INFO file with control measurements to the output directory based on the query plan */
  private def writeInfoFileForQueryForSdkS3(qe: QueryExecution, region: Region, kmsSettings: S3KmsSettings)(implicit credentialsProvider: AwsCredentialsProvider): Unit = {
    val infoFilePath = ExecutionPlanUtils.inferOutputInfoFileNameOnS3(qe, cf.outputInfoFileName)

    // Write _INFO file to the output directory
    infoFilePath.foreach(path => {

      import za.co.absa.atum.persistence.s3.S3LocationRegionImplicits.SimpleS3LocationRegionExt
      import za.co.absa.atum.location.S3Location.StringS3LocationExt

      val location = path.toS3LocationOrFail.withRegion(region)

      AtumSdkS3.log.debug(s"Inferred _INFO Location = $location")
      cf.storeCurrentInfoFileOnSdkS3(location, kmsSettings)
    })

    // Write _INFO file to a registered storer
    if (cf.accumulator.isStorerLoaded) {
      cf.accumulator.store()
    }
  }

}
