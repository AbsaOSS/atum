package za.co.absa.atum.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.mockito.Mockito
import org.mockito.scalatest.IdiomaticMockito
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExecutionPlanUtilsSuite extends AnyFlatSpec with Matchers with IdiomaticMockito {

  val hadoopConf = new Configuration

  implicit class SimplePath(path: Path) {
    // disregarding hdfs nameserver prefix or local FS fallback (file://)
    def simplePath: String = path.toUri.getPath
  }

  "inferOutputInfoFileName" should "derive output file name for HDFS from SaveIntoDataSourceCommand" in {
    val qe = mock[QueryExecution]
    Mockito.when(qe.analyzed).thenReturn(
      SaveIntoDataSourceCommand(null, null, options = Map(("path", "/tmp")), null)
    )

    ExecutionPlanUtils.inferOutputFileName(qe, hadoopConf).get.simplePath shouldBe "/tmp"
  }

  "inferOutputInfoFileName" should "derive output info file name for HDFS from SaveIntoDataSourceCommand" in {
    val qe = mock[QueryExecution]
    val myInfoName = "myInfo"
    Mockito.when(qe.analyzed).thenReturn(
      SaveIntoDataSourceCommand(null, null, options = Map(("path", "/tmp/here")), null)
    )

    ExecutionPlanUtils.inferOutputInfoFileName(qe, myInfoName).get.simplePath shouldBe "/tmp/here/myInfo"
  }

  "inferOutputInfoFileNameOnS3" should "derive output info file name for S3 from SaveIntoDataSourceCommand" in {
    val qe = mock[QueryExecution]
    val myInfoName = "myInfo"
    Mockito.when(qe.analyzed).thenReturn(
      // trailing slash should get taken care of
      SaveIntoDataSourceCommand(null, null, options = Map(("path", "/tmp/here2/")), null)
    )

    ExecutionPlanUtils.inferOutputInfoFileNameOnS3(qe, myInfoName).get shouldBe "/tmp/here2/myInfo"
  }



}
