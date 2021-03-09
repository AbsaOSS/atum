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

package za.co.absa.atum.utils

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import za.co.absa.atum.core.AtumSdkS3.log

object SdkS3ClientUtils {

  def getLocalProfileCredentialsProvider(credentialsProfileName: String): ProfileCredentialsProvider = {
    val localProfileCredentials = ProfileCredentialsProvider.create(credentialsProfileName)
    log.debug(s"Credentials of local $credentialsProfileName profile =" +
      s" ${localProfileCredentials.resolveCredentials().accessKeyId()}, ${localProfileCredentials.resolveCredentials().secretAccessKey().take(5)}...")

    localProfileCredentials
  }

  def getS3Client(region: Region, credentialsProvider: AwsCredentialsProvider): S3Client = {
    S3Client.builder()
      .region(region)
      .credentialsProvider(credentialsProvider)
      .build()
  }

}
