package za.co.absa.atum.utils

object FileUtils {
  def readFileToString(path: String): String = {
    val testTxtSource = scala.io.Source.fromFile(path)
    val str = testTxtSource.mkString
    testTxtSource.close()

    str
  }

}
