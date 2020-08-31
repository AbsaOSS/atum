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

package za.co.absa.atum.persistence.s3

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.{ControlMeasuresParser, ControlMeasuresStorer, S3KmsSettings, S3Location}
import za.co.absa.atum.utils.S3Utils

/**
 * A storer of control measurements to a JSON file stored in AWS S3.
 *
 * @param outputLocation      s3 location to save measurements data to
 * @param kmsSettings         KMS settings - server side encryption configuration
 * @param credentialsProvider a specific credentials provider (e.g. SAML profile). Consider using [[DefaultCredentialsProvider#create()]] when in doubt.
 */
class ControlMeasuresS3StorerJsonFile(outputLocation: S3Location, kmsSettings: S3KmsSettings)
                                     (implicit credentialsProvider: AwsCredentialsProvider) extends ControlMeasuresStorer {

  /**
   * Stores the `controlInfo` measurement to an S3 location.
   *
   * @param controlInfo measurements to store
   * @throws SdkException when storing fails.
   */
  override def store(controlInfo: ControlMeasure): Unit = {
    val serialized = ControlMeasuresParser asJson controlInfo
    saveDataToFile(serialized)
  }

  private def saveDataToFile(data: String): Unit = {
    val s3Client = getS3Client

    val putRequest = PutObjectRequest.builder.bucket(outputLocation.bucketName).key(outputLocation.path)
      .serverSideEncryption(kmsSettings.serverSideEncryption)
      .ssekmsKeyId(kmsSettings.kmsKeyId)
      .build()

    // would throw S3Exception or SdkClientException in case of failure (base exception class: SdkException)
    s3Client.putObject(putRequest, RequestBody.fromString(data))
  }

  override def getInfo: String = {
    s"JSON serializer for Storer to ${outputLocation.s3String()}"
  }

  private[s3] def getS3Client: S3Client = S3Utils.getS3Client(outputLocation.region, credentialsProvider)
}
