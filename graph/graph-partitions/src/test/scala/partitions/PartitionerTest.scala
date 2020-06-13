package partitions

import org.apache.spark.{SparkConf, SparkContext}

object PartitionerTest {

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Please enter parameters!")
      println(
        """
          |Usage: UseMyPartitioner
          |Param:
          |      inputGraphPath
          |      partNum
        """.stripMargin)
      sys.exit(-1) // -1 非正常退出
    }

    val inputGraphPath = args(0)
    val partNum = args(1).toInt

    //    val conf = new SparkConf().setMaster("local[4]").setAppName("UseMyPartitioner")
    val conf = new SparkConf().setAppName("UseMyPartitioner")
    val sc = new SparkContext(conf)

    UsePartitioner.execute(sc, partNum, inputGraphPath)
  }

}
