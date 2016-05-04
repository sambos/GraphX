package org.rsol.poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object GraphXExample {
  
  val vertexFile = "./src/test/resources/vertices.csv"
  val edgeFile = "./src/test/resources/edges.csv"

  case class NodeProperties(nodeId:String, userId:String, url:String)
  
  def main(args:Array[String]) {
    
    val sc:SparkContext = new SparkContext("local", "graphx",new SparkConf())
    
    val vertexRDD: RDD[(Long, NodeProperties)] = sc.textFile(vertexFile).map(line => line.split(",")).map(x => (x(0).hashCode, NodeProperties(x(0),x(1), x(2))))
    val edgeRDD: RDD[Edge[String]] = sc.textFile(edgeFile).map(line => line.split(",")).map(x => Edge(x(0).hashCode,x(1).hashCode,x(2)))    
    
    val graph: Graph[NodeProperties, String] = Graph(vertexRDD, edgeRDD)
    
    println(graph.vertices.count())
    println(graph.edges.count())
    
    graph.vertices.filter( _._2 != null).foreach {
      case(id,x) => println(s"$id ${x.userId} ${x.url}" ); 
      }

    println("triplet count " + graph.triplets.count())
    graph.triplets.filter(_.srcAttr != null).map(triplet =>
    triplet.srcAttr.label + " --> " + triplet.attr  + " ---> " + triplet.dstAttr.label + " with user " + triplet.srcAttr.userId).collect().foreach { println _ }

    graph.triplets.filter(_.srcAttr != null).filter(x => x.srcAttr.userId == "sambos").map(triplet =>
    triplet.srcAttr.label + " --> " + triplet.attr  + " --> " + triplet.dstAttr.label + " with user " + triplet.srcAttr.userId).collect().foreach { println _ }
    
    sc.stop
  }
}
