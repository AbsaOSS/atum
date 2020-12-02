package za.co.absa.atum.utils

object S3LocationUtils {

  // hint: https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
  val S3LocationRx = "(s3[an]?)://([-a-z0-9.]{3,63})/(.*)".r

  implicit class StringS3LocationExt(path: String) {

    def toS3Location: Option[SimpleS3Location] = {
      path match {
        case S3LocationRx(protocol, bucketName, path) => Some(SimpleS3Location(protocol, bucketName, path))
        case _ => None
      }
    }

    def toS3LocationOrFail: SimpleS3Location = {
      this.toS3Location.getOrElse{
        throw new IllegalArgumentException(s"Could not parse S3 Location from $path using rx $S3LocationRx.")
      }
    }

    def isValidS3Path: Boolean = path match {
      case S3LocationRx(_, _, _) => true
      case _ => false
    }
  }
}

trait S3Location {
  def protocol: String
  def bucketName: String
  def path: String

  /**
   * Returns formatted S3 string, e.g. `s3://myBucket/path/to/somewhere`
   * @return formatted s3 string
   */
  def s3String: String = s"$protocol://$bucketName/$path"
}

case class SimpleS3Location(protocol: String, bucketName: String, path: String) extends S3Location
