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

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import za.co.absa.atum.core.AtumSdkS3
import za.co.absa.atum.persistence.s3.{ControlMeasuresSdkS3LoaderJsonFile, ControlMeasuresSdkS3StorerJsonFile, S3KmsSettings, SimpleS3LocationWithRegion}

import scala.language.implicitConversions

/**
  * The object contains implicit methods for Control Framework
  */
object AtumImplicitsSdkS3 {


  /**
    * The class contains implicit methods for [[org.apache.spark.sql.SparkSession]].
    */
  implicit class SparkSessionWrapperSdkS3(sparkSession: SparkSession)(implicit atum: AtumSdkS3) {

    import za.co.absa.atum.AtumImplicits.SparkSessionWrapper

    /**
     * Enable S3-based control measurements tracking via SDK S3
     *
     * @param sourceS3Location    s3 location to load info files from in S3
     * @param destinationS3Config s3 location and kms settings to save the data to in S3
     * @param credentialsProvider If you do not have a specific Credentials provider, use the default
     *                            { @link software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider#create()}
     * @return spark session with atum tracking enabled
     */
    def enableControlMeasuresTrackingForSdkS3(sourceS3Location: Option[SimpleS3LocationWithRegion],
                                              destinationS3Config: Option[(SimpleS3LocationWithRegion, S3KmsSettings)])
                                             (implicit credentialsProvider: AwsCredentialsProvider): SparkSession = {

      val loader = sourceS3Location.map(ControlMeasuresSdkS3LoaderJsonFile(_))
      val storer = destinationS3Config.map { case (destLoc, kms) =>
        ControlMeasuresSdkS3StorerJsonFile(destLoc, kms)
      }

      sparkSession.enableControlMeasuresTracking(loader, storer)
    }

  }

  /**
    * The class contains implicit methods for [[org.apache.spark.sql.Dataset]].
    */
  implicit class DataSetWrapperSdkS3(dataset: Dataset[Row])(implicit atum: AtumSdkS3) {

    def writeInfoFileOnS3(s3Location: SimpleS3LocationWithRegion, s3KmsSettings: S3KmsSettings)(implicit credentialsProvider: AwsCredentialsProvider): Dataset[Row] = {
      atum.controlFrameworkStateSdkS3.storeCurrentInfoFileOnSdkS3(s3Location, s3KmsSettings)
      dataset
    }

  }

}
