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

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import za.co.absa.commons.s3.{S3Location, SimpleS3Location}

trait Regional {
  def region: Region
}

case class SimpleS3LocationWithRegion(protocol: String, bucketName: String, path: String, region: Region) extends S3Location with Regional {
  def withRegion(region: Region): SimpleS3LocationWithRegion = this.copy(region = region)

  override def asSimpleS3LocationString: String = SimpleS3Location(protocol, bucketName, path).asSimpleS3LocationString
}

case class S3KmsSettings(kmsKeyId: String, serverSideEncryption: ServerSideEncryption = ServerSideEncryption.AWS_KMS)

object S3LocationRegionImplicits {

  implicit class SimpleS3LocationRegionExt(s3Loc: S3Location) {
    def withRegion(region: Region): SimpleS3LocationWithRegion =
      SimpleS3LocationWithRegion(s3Loc.protocol, s3Loc.bucketName, s3Loc.path, region)
  }

}

