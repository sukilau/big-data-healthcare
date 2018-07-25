/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))
    var current_idx = patients.map(x => x.patientID).max().toLong+1

    /** Making Diagnostics Vertices*/
    val diagVertexIdRDD: RDD[(String, VertexId)] = diagnostics.map(_.icd9code).distinct.zipWithIndex
        .map{case(icd9code, idx) => (icd9code, idx + current_idx)}
    val diag2VertexId: Map[String, VertexId] = diagVertexIdRDD.collect.toMap
    val vertexDiagnostic: RDD[(VertexId, VertexProperty)] = diagVertexIdRDD.
      map{case(icd9code, idx) => (idx, DiagnosticProperty(icd9code))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]
    var current_idx_diag = current_idx + diagVertexIdRDD.count()+1

    /** Making LabResults Vertices*/
    val labVertexIdRDD: RDD[(String, VertexId)] = labResults.map(_.labName).distinct.zipWithIndex
        .map{case(labName, idx) => (labName, idx + current_idx_diag)}
    val lab2VertexId: Map[String, VertexId] = labVertexIdRDD.collect.toMap
    val vertexLab: RDD[(VertexId, VertexProperty)] = labVertexIdRDD.
      map{case(labName, index) => (index, LabResultProperty(labName))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]
    var current_idx_lab = current_idx_diag + (labVertexIdRDD.count()+1)

    /** Making Medication Vertices*/
    val medVertexIdRDD: RDD[(String, VertexId)] = medications.map(_.medicine).distinct.zipWithIndex
      .map{case(medicine, idx) => (medicine, idx + current_idx_lab)}
    val med2VertexId: Map[String, VertexId] = medVertexIdRDD.collect.toMap
    val vertexMedication: RDD[(VertexId, VertexProperty)] = medVertexIdRDD.
      map{case(medicine, index) => (index, MedicationProperty(medicine))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]
    var current_idx_med = current_idx_lab + (medVertexIdRDD.count()+1)

    /** HINT: See Example of Making PatientPatient Edges Below
      *
      * This is just sample edges to give you an example.
      * You can remove this PatientPatient edges and make edges you really need
      * */
//    case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty
//    val edgePatientPatient: RDD[Edge[EdgeProperty]] = patients
//      .map({p =>
//        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
//      })
    val sc = diagnostics.sparkContext
    val bcDiag2VertexId = sc.broadcast(diag2VertexId)
    val bcLab2VertexId = sc.broadcast(lab2VertexId)
    val bcMed2VertexId = sc.broadcast(med2VertexId)

    /** Making Patient-Diag Edges*/
    val edgeDiagnostic: RDD[Edge[EdgeProperty]] = {
      diagnostics.map(x => ((x.patientID, x.icd9code), x.date, x.sequence))
        .keyBy(_._1)
        .reduceByKey((x1, x2) => if (x1._2 > x2._2) x1 else x2)
        .map(x => Edge(
          x._1._1.toLong,
          bcDiag2VertexId.value(x._1._2).toLong,
          PatientDiagnosticEdgeProperty(Diagnostic(x._1._1, x._2._2, x._1._2, x._2._3)).asInstanceOf[EdgeProperty])
        )
    }

    /** Making Patient-Lab Edges*/
    val edgeLab: RDD[Edge[EdgeProperty]] = {
      labResults.map(x => ((x.patientID, x.labName), x.date, x.value))
        .keyBy(_._1)
        .reduceByKey((x1, x2) => if (x1._2 > x2._2) x1 else x2)
        .map(x => Edge(
          x._1._1.toLong,
          bcLab2VertexId.value(x._1._2).toLong,
          PatientLabEdgeProperty(LabResult(x._1._1, x._2._2, x._1._2, x._2._3)).asInstanceOf[EdgeProperty])
        )
    }

    /** Making Patient-Med Edges*/
    val edgeMedication: RDD[Edge[EdgeProperty]] = {
      medications.map(x => ((x.patientID, x.medicine), x.date))
        .keyBy(_._1)
        .reduceByKey((x1, x2) => if (x1._2 > x2._2) x1 else x2)
        .map(x => Edge(
          x._1._1.toLong,
          bcMed2VertexId.value(x._1._2).toLong,
          PatientMedicationEdgeProperty(Medication(x._1._1, x._2._2, x._1._2)).asInstanceOf[EdgeProperty])
        )
    }

    /** Making Vertices and Edges*/
    val vertices: RDD[(VertexId, VertexProperty)] = sc.union(vertexPatient, vertexDiagnostic, vertexMedication, vertexLab)
    val edgeDiag_bi: RDD[Edge[EdgeProperty]] = edgeDiagnostic.union(edgeDiagnostic.map(x => Edge(x.dstId,x.srcId,x.attr)))
    val edgeLab_bi: RDD[Edge[EdgeProperty]] = edgeLab.union(edgeLab.map(x => Edge(x.dstId,x.srcId,x.attr)))
    val edgeMed_bi: RDD[Edge[EdgeProperty]] = edgeMedication.union(edgeMedication.map(x => Edge(x.dstId,x.srcId,x.attr)))
    val edges: RDD[Edge[EdgeProperty]] = edgeDiag_bi.union(edgeLab_bi).union(edgeMed_bi)

    /** Making Graph*/
    // Making Graph
//    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertexPatient, edgePatientPatient)
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)

    graph
  }
}
