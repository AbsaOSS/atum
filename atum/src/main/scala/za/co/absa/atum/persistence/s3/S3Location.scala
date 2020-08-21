package za.co.absa.atum.persistence

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.ServerSideEncryption

case class S3Location(bucketName: String, path: String, region: Region = Region.EU_WEST_1) {
  /**
   * Returns formatted S3 string, e.g. `s3://myBucket/path/to/somewhere`
   * @param protocol http "s3" protocol, e.g. s3, s3n, s3a. Default = "s3".
   * @return formatted s3 string
   */
  def s3String(protocol: String = "s3"): String = s"s3://$bucketName/$path"
}

case class S3KmsSettings(kmsKeyId: String, serverSideEncryption: ServerSideEncryption = ServerSideEncryption.AWS_KMS)

