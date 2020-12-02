package za.co.absa.atum.persistence

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import za.co.absa.atum.utils.{S3Location, SimpleS3Location}

trait Regional {
  def region: Region
}

case class SimpleS3LocationWithRegion(protocol: String, bucketName: String, path: String, region: Region) extends S3Location with Regional {
  def withRegion(region: Region): SimpleS3LocationWithRegion = this.copy(region = region)
}

case class S3KmsSettings(kmsKeyId: String, serverSideEncryption: ServerSideEncryption = ServerSideEncryption.AWS_KMS)

object S3LocationRegionImplicits {

  implicit class SimpleS3LocationRegionExt(s3Loc: S3Location) {
    def withRegion(region: Region): SimpleS3LocationWithRegion =
      SimpleS3LocationWithRegion(s3Loc.protocol, s3Loc.bucketName, s3Loc.path, region)
  }

}

