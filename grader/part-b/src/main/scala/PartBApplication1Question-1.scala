import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PartBApplication1Question1 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(
        "Usuage: PartBApplication1Question1 <edge_list_file>")
      System.exit(-1)
    }
    val spark = SparkSession
        .builder
        .master("spark://10.254.0.23:7077")
        .appName("PartBApplication1Question1")
        .config("spark.driver.memory", "1g")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/home/ubuntu/logs/apps/")
        .config("spark.executor.memory", "16g")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "1")
        .getOrCreate()
    val sc = spark.sparkContext
  
    val file_name = args(0)
    val iters = 20
    val unpartgraph = GraphLoader.edgeListFile(sc, file_name).cache()
    val graph = unpartgraph.partitionBy(PartitionStrategy.RandomVertexCut)
    
    val pr = run(graph, iters).vertices.cache()
    val ranks = pr.map{ case (id, r) => id + "\t" + r }

    ranks.collect.foreach(println(_))
  }

  def run(graph:Graph[Int,Int], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    val pagerankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets( e => 1.0 / e.srcAttr )
      .mapVertices( (id, attr) => 1.0 )
      .cache()

    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = 
      resetProb + (1.0 - resetProb) * msgSum
  
    def sendMessage(edge: EdgeTriplet[Double,Double]) = 
      Iterator((edge.dstId, edge.srcAttr * edge.attr))
    
    def messageCombiner(a: Double, b: Double): Double = a+b

    val initialMessage = 0.0

    Pregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  }
}
