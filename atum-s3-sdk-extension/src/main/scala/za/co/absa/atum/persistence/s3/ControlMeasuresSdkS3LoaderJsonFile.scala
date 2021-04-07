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

package za.co.absa.atum.persistence.s3

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.{ControlMeasuresLoader, ControlMeasuresParser}
import za.co.absa.atum.utils.SdkS3ClientUtils
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils

/**
 * A loader of control measurements from a JSON file stored in AWS S3.
 * @param inputLocation S3 location to read the json measurements from
 * @param credentialsProvider a specific credentials provider (e.g. SAML profile). Consider using [[DefaultCredentialsProvider#create()]] when in doubt.
 */
case class ControlMeasuresSdkS3LoaderJsonFile(inputLocation: SimpleS3LocationWithRegion)
                                        (implicit credentialsProvider: AwsCredentialsProvider) extends ControlMeasuresLoader {
  override def load(): ControlMeasure = {
    val s3Client: S3Client = getS3Client

    val getRequest = GetObjectRequest
      .builder().bucket(inputLocation.bucketName).key(inputLocation.path)
      .build()

    val controlInfoJson = s3Client.getObjectAsBytes(getRequest).asUtf8String()
    ControlMeasureUtils.preprocessControlMeasure(ControlMeasuresParser fromJson controlInfoJson)
  }

  override def getInfo: String = {
    s"JSON deserializer from ${inputLocation.asSimpleS3LocationString}"
  }

  private[s3] def getS3Client: S3Client = SdkS3ClientUtils.getS3Client(inputLocation.region, credentialsProvider)

}
