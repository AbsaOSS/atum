package za.co.absa.atum.persistence

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.ServerSideEncryption

trait S3Location {
  def bucketName: String
  def path: String

  def withRegion(region: Region): S3Location with Regionable

  /**
   * Returns formatted S3 string, e.g. `s3://myBucket/path/to/somewhere`
   * @param protocol http "s3" protocol, e.g. s3, s3n, s3a. Default = "s3".
   * @return formatted s3 string
   */
  def s3String(protocol: String = "s3"): String = s"s3://$bucketName/$path"
}

trait Regionable {
  def region: Region
}

case class SimpleS3Location(bucketName: String, path: String) extends S3Location {
  override def withRegion(region: Region): SimpleS3LocationWithRegion =
    SimpleS3LocationWithRegion(bucketName, path, region)
}

case class SimpleS3LocationWithRegion(bucketName: String, path: String, region: Region) extends S3Location with Regionable {
  override def withRegion(region: Region): SimpleS3LocationWithRegion = this.copy(region = region)
}

case class S3KmsSettings(kmsKeyId: String, serverSideEncryption: ServerSideEncryption = ServerSideEncryption.AWS_KMS)

