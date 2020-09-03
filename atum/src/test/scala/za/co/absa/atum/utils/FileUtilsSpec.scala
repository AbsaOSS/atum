package za.co.absa.atum.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileUtilsSpec extends AnyFlatSpec with Matchers {

  "PathJoin" should "join paths correctly" in {

    import za.co.absa.atum.utils.FileUtils.PathJoin
    "/path/to" / "file" shouldBe "/path/to/file"
    "/path/to/" / "file" shouldBe "/path/to/file"
    "/path/to" / "/file" shouldBe "/path/to/file"
    "/path/to/" / "/file" shouldBe "/path/to/file"

  }
}
