import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object PartBApplication2Question6 {
  def main(args: Array[String]) {
    
    val spark = SparkSession
        .builder
        .master("spark://10.254.0.23:7077")
        .appName("PartBApplication2Question6")
        .config("spark.driver.memory", "1g")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/home/ubuntu/logs/apps/")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "1")
        .getOrCreate()
    val sc = spark.sparkContext
  
    val vfile = sc.textFile("vertices.txt")
    val vRDD: RDD[(VertexId, Array[String])] = vfile.map(line => line.split(","))
            .zipWithIndex()
            .map(_.swap)

    val eRDD:RDD[Edge[(VertexId, VertexId)]] = vRDD.cartesian(vRDD)
                  .filter(x => (x._1._1 != x._2._1) && checkCommonString(x._1._2, x._2._2) == true)
                  .map(x => Edge(x._1._1, x._2._1))

    val graph = Graph(vRDD, eRDD)
 
    val group:RDD[(Long,String)] = graph.triplets.map( triplet  =>
      (triplet.srcId, triplet.srcAttr.mkString(","))
    ).reduceByKey( (a,b) => a)

    val group_contribs = group.flatMap{ case (id, words) => 
      val parts = words.split(",")
      parts.map(part => (part, 1))
    }
    val contribs_reduc = group_contribs.reduceByKey(_+_).reduce(max)
    val most_pop_word = contribs_reduc._1

    val timeinterval:RDD[(Long,Int)] = graph.triplets.map( triplet => 
      if (triplet.srcAttr.contains(most_pop_word)) (triplet.srcId, 1) else (triplet.srcId, 0)
    ).reduceByKey((a,b) => a)
    val total_timeintervals = timeinterval.reduce(aggregate)
    println("Number of time intervals with most popular word: " + total_timeintervals._2)
  }
  
  def checkCommonString(a: Array[String], b: Array[String]): Boolean =  {
    val aSet = a.toSet
    val bSet = b.toSet
    val unionSet = aSet & bSet
    return if (unionSet.size == 0) false else true
  }

  def max(a: (String, Int), b: (String, Int)): (String, Int) = {
    return if (a._2 > b._2) a else b
  }

  def aggregate(a: (Long, Int), b: (Long, Int)): (Long, Int) = {
    return (a._1, a._2 + b._2)
  }
}
