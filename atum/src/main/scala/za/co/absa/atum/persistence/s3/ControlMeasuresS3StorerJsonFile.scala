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

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse, ServerSideEncryption}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.{ControlMeasuresParser, ControlMeasuresStorer, S3KmsSettings, S3Location}

/** A storer of control measurements to AWS S3 as a JSON file . */
class ControlMeasuresS3StorerJsonFile(outputLocation: S3Location, kmsSettings: S3KmsSettings) extends ControlMeasuresStorer {
  override def store(controlInfo: ControlMeasure): Unit = {
    val serialized =  ControlMeasuresParser asJson controlInfo
    saveDataToFile(serialized)
  }

  private def saveDataToFile(data: String): Unit = {
    // to run locally, we need credentials:
    val samlCredentials = ProfileCredentialsProvider.create("saml")
    println(s"samlCredentials = ${samlCredentials.resolveCredentials().accessKeyId()}, ${samlCredentials.resolveCredentials().secretAccessKey().take(5)}...")

    val s3Client = S3Client.builder()
      .region(outputLocation.region)
      .credentialsProvider(samlCredentials) // todo only for local? use default credentials instead?
      .build()

    val putRequest = PutObjectRequest.builder.bucket(outputLocation.bucketName).key(outputLocation.path)
      .serverSideEncryption(kmsSettings.serverSideEncryption)
      .ssekmsKeyId(kmsSettings.kmsKeyId)
      .build()

    // may throw S3Exception or SdkClientException (base exception class = SdkException)
    s3Client.putObject(putRequest, RequestBody.fromString(data)) // would throw S3Exception or similar
  }

  override def getInfo: String = {
    s"JSON serializer for Storer to ${outputLocation.s3String()}"
  }
}
