package newpartitions

import java.io.FileWriter
import java.util.Date

import org.apache.spark.graphx.PartitionStrategy.{CanonicalRandomVertexCut, EdgePartition1D, EdgePartition2D, RandomVertexCut}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

object OLPA {

  /**
    * Run Overlapping Community Detection for detecting overlapping communities in networks.
    *
    * OLPA is an overlapping community detection algorithm.It is based on standarad Label propagation
    * but instead of single community per node , multiple communities can be assigned per node.
    *
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph           the graph for which to compute the community affiliation
    * @param maxSteps        the number of supersteps of OLPA to be performed. Because this is a static
    *                        implementation, the algorithm will run for exactly this many supersteps.
    * @param noOfCommunities the maximum number of communities to be assigned to each vertex
    * @return a graph with list of vertex attributes containing the labels of communities affiliation
    */

  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int, noOfCommunities: Int) = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")
    require(noOfCommunities > 0, s"Number of communities must be greater than 0, but got ${noOfCommunities}")

    val threshold: Double = 1.0 / noOfCommunities

    val lpaGraph: Graph[mutable.Map[VertexId, Double], ED] = graph.mapVertices { case (vid, _) => mutable.Map[VertexId, Double](vid -> 1) }
    println("lpaGraph结束")

    def sendMessage(e: EdgeTriplet[mutable.Map[VertexId, Double], ED]): Iterator[(VertexId, mutable.Map[VertexId, Double])] = {
      println("sendMessage")
      Iterator((e.srcId, e.dstAttr), (e.dstId, e.srcAttr))
    }

    def mergeMessage(count1: mutable.Map[VertexId, Double], count2: mutable.Map[VertexId, Double])
    : mutable.Map[VertexId, Double] = {
      println("mergeMessage")
      val communityMap: mutable.Map[VertexId, Double] = new mutable.HashMap[VertexId, Double]
      (count1.keySet ++ count2.keySet).map(key => {

        val count1Val = count1.getOrElse(key, 0.0)
        val count2Val = count2.getOrElse(key, 0.0)
        communityMap += (key -> (count1Val + count2Val))
      })
      communityMap
    }

    def vertexProgram(vid: VertexId, attr: mutable.Map[VertexId, Double], message: mutable.Map[VertexId, Double]): mutable.Map[VertexId, Double] = {
      if (message.isEmpty)
        attr
      else {
        println("vertexProgram")
        var coefficientSum = message.values.sum
        println("通信边数：" + coefficientSum)
        //Normalize the map so that every node has total coefficientSum as 1
        val normalizedMap: mutable.Map[VertexId, Double] = message.map(row => {
          (row._1 -> (row._2 / coefficientSum))
        })


        val resMap: mutable.Map[VertexId, Double] = new mutable.HashMap[VertexId, Double]
        var maxRow: VertexId = 0L
        var maxRowValue: Double = Double.MinValue

        normalizedMap.foreach(row => {
          if (row._2 >= threshold) {
            resMap += row
          } else if (row._2 > maxRowValue) {
            maxRow = row._1
            maxRowValue = row._2
          }
        })

        //Add maximum value node in result map if there is no node with sum greater then threshold
        if (resMap.isEmpty) {
          resMap += (maxRow -> maxRowValue)
        }

        coefficientSum = resMap.values.sum
        resMap.map(row => {
          (row._1 -> (row._2 / coefficientSum))
        })
      }
    }

    val initialMessage = mutable.Map[VertexId, Double]()

    val overlapCommunitiesGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    overlapCommunitiesGraph.mapVertices((vertexId, vertexProperties) => vertexProperties.keys)
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      println("Please enter parameters!")
      println(
        """
          |Usage: OLPA
          |Param:
          |      inputPath
          |      outputPathTime
          |      maxSteps
          |      noOfCommunities
          |      numParts
          |      tag
        """.stripMargin)
      sys.exit(-1) // -1 非正常退出
    }

    val inputPath = args(0)
    val outputPathTime = args(1)
    val maxSteps = args(2).toInt
    val noOfCommunities = args(3).toInt
    val numParts = args(4).toInt
    val tag = args(5).toInt
    val conf = new SparkConf().setMaster("local").setAppName("Partitioner COPRA")
    //    val conf = new SparkConf().setAppName("Partitioner COPRA")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, inputPath)

    var newGraph = graph

    if (tag == 0) { // 默认图Hash分区
      println("GraphX默认图Hash分区方式！")
      newGraph = graph
    } else if (tag == 1) { // EdgePartition2D
      println("GraphX EdgePartition2D 分区方式！")
      newGraph = graph.partitionBy(EdgePartition2D, numParts)
    } else if (tag == 2) { // EdgePartition1D
      println("GraphX EdgePartition1D 分区方式！")
      newGraph = graph.partitionBy(EdgePartition1D, numParts)
    } else if (tag == 3) { // RandomVertexCut
      println("GraphX RandomVertexCut 分区方式！")
      newGraph = graph.partitionBy(RandomVertexCut, numParts)
    } else if (tag == 4) { // CanonicalRandomVertexCut
      println("GraphX CanonicalRandomVertexCut 分区方式！")
      newGraph = graph.partitionBy(CanonicalRandomVertexCut, numParts)
    } else if (tag == 5) { // DegreePartition
      println("GraphX DegreePartition 分区方式！")
      newGraph = graph.partitionBy(DegreePartition, numParts)
    } else if (tag == 6) { // MetisPartition
      println("GraphX MetisPartition 分区方式！")
      newGraph = graph.partitionBy(MetisPartition, numParts)
    } else if (tag == 7) {
      newGraph = graph.partitionBy(
        new PartitionStrategy {

          override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {

          }
        }
      )
    } else {
      println("Not This Option!")
      sys.exit(-1)
    }


    var start_time = new Date().getTime
    val overlapCommunities = OLPA.run(newGraph, maxSteps, noOfCommunities)

    var end_time = new Date().getTime
    //    val time = new FileWriter(outputPathTime)
    println("运行时间：" + (end_time - start_time) + "ms")
    //    time.write((end_time - start_time) + " ms" + "\r\n")
    //    time.close()

  }

}
