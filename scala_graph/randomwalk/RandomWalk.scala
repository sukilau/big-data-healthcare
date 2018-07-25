package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model.{PatientProperty, EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /**
    Given a patient ID, compute the random walk probability w.r.t. to all other patients.
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */
    val allPatientIDs = graph.vertices.filter(x => x._2.isInstanceOf[PatientProperty]).map(_._1.toLong).collect.toSet
    val src: VertexId = patientID

    /**initialize PageRank graph*/
    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
      .mapTriplets(triplets => 1.0/triplets.srcAttr, TripletFields.Src)
      .mapVertices((id, attr) => if (!(id != src)) alpha else 0.0)

    /**Random Walk Algorithm*/
    var i = 0
    var prevRankGraph: Graph[Double, Double] = null

    while (i < numIter) {
      rankGraph.cache()

      val rankUpdates = rankGraph.aggregateMessages[Double](
        triplet => triplet.sendToDst(triplet.srcAttr * triplet.attr), _ + _, TripletFields.Src)

      prevRankGraph = rankGraph

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => if(id==patientID) alpha+(1.0-alpha)*msgSum else (1.0-alpha)*msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {})
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      i += 1
    }

    val top10 = rankGraph.vertices
      .filter(x => allPatientIDs.contains(x._1.toLong))
      .takeOrdered(11)(Ordering[Double].reverse.on(x => x._2))
      .map(_._1)

    /** Remove this placeholder and implement your code */
//    List(1,2,3,4,5)
    top10.slice(1, top10.length).toList
  }
}
