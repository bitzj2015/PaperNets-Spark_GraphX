import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
object PaperNets {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    //Initialization
    val conf = new SparkConf().setAppName("PaperNets").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //Read data from dataset(containing several texts)
    val df = sqlContext.read.json("E:\\PaperNets\\dataset\\*.txt")
    val ID = df.select("id","issue","title").toDF("id","issue","title")
    val Relation = df.select(df("id"), explode(df("references")),df("year")).toDF("id", "references","year")
    val ID_rdd = ID.collect()
    val Relation_rdd = Relation.collect()
    println(ID_rdd.length)
    //Create Vertex(id, reference, year)
    var vertexArr = new ArrayBuffer[(Long, (String, String,String))]()
    var count = 0
    var a1 = ""
    var a2 = ""
    var a3 = ""
    while (count < ID_rdd.length) {
      //Handle null string
      if (ID_rdd(count).get(0) == null) {
        a1 = ""
      } else {
        a1 = ID_rdd(count).get(0).toString
      }
      if (ID_rdd(count).get(1) == null) {
        a2 = ""
      } else {
        a2 = ID_rdd(count).get(1).toString
      }
      if (ID_rdd(count).get(2) == null) {
        a3 = ""
      } else {
        a3 = ID_rdd(count).get(2).toString
      }
      vertexArr += ((count + 1, (a1, a2, a3)))
      count = count + 1
    }
    println(count)
    //Create Edge(data)
    val edgeArr = new ArrayBuffer[Edge[String]]()
    var id1 = Relation_rdd(0).get(0).toString
    var id2 = Relation_rdd(0).get(1).toString
    var date = Relation_rdd(0).get(2).toString
    var start: Long = 0
    var end: Long = 0
    count = 0
    while (count < Relation_rdd.length) {
      //Handle null string
      if (Relation_rdd(count).get(0) == null) {
        id1 = ""
      } else {
        id1 = Relation_rdd(count).get(0).toString
      }
      if (Relation_rdd(count).get(1) == null) {
        id2 = ""
      } else {
        id2 = Relation_rdd(count).get(1).toString
      }
      if (Relation_rdd(count).get(2) == null) {
        date = ""
      } else {
        date = Relation_rdd(count).get(2).toString
      }
      start = -1
      end = -1
      var index = 0
      var flag = 0
      while ((start<0 || end<0)&&index<vertexArr.length) {
        //Find edges
        if (vertexArr(index)._2._1 == id1) {
          start = index
        }
        if (vertexArr(index)._2._1 == id2) {
          end = index
        }
        index = index + 1
      }
      if (start >= 0 && end >= 0) {
        edgeArr += Edge(start, end, date)
      }
      count += 1
      if (count % 1000 == 0) {
        println(count + '\n')
      }
    }
    println(edgeArr.length)
    //Create graph
    val papers: RDD[(VertexId, (String, String,String))] = sc.parallelize(vertexArr)
    val relationships: RDD[Edge[String]] = sc.parallelize(edgeArr)
    val graph = Graph(papers,relationships)
    //Pagerank
    val ranks = graph.pageRank(0.001).vertices
    print(ranks.take(10))
    //Find ConnectedComponents and print them
    val comps = graph.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect()
    for (i <- 0.to(comps.length - 1)) {
      println(comps(i))
    }
    papers.saveAsTextFile("E:\\PaperNets\\papers_id.txt")
    relationships.saveAsTextFile("E:\\PaperNets\\relationships_id.txt")
  }
}