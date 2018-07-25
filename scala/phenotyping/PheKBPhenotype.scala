/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

object T2dmPhenotype {
  
  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext

    /** Hard code the criteria */
    // val type1_dm_dx = Set("code1", "250.03")
    // val type1_dm_med = Set("med1", "insulin nph")
    // use the given criteria above like T1DM_DX, T2DM_DX, T1DM_MED, T2DM_MED and hard code DM_RELATED_DX criteria as well
    val type1_dm_dx = Set("code1", "250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43","250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")
    val type2_dm_dx = Set("code2", "250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6", "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")
    val type1_dm_med = Set("med1", "lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")
    val type2_dm_med = Set("med2", "chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl", "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl", "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose", "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide", "avandia", "actos", "actos", "glipizide")
    val dm_related_dx = Set("790.21","790.22","790.2","790.29","648.81","648.82","648.83","648.84","648.0","648.00","648.01","648.02","648.03","648.04","791.5","277.7","V77.1","256.4")
    
    
    /** Find CASE Patients */
    // val casePatients = sc.parallelize(Seq(("casePatient-one", 1), ("casePatient-two", 1), ("casePatient-three", 1)))
    val t1dx = diagnostic.filter(x => type1_dm_dx.contains(x.code)).map(x => x.patientID)
    val t2dx = diagnostic.filter(x => type2_dm_dx.contains(x.code)).map(x => x.patientID)
    val t1med = medication.filter(x => type1_dm_med.contains(x.medicine)).map(x => x.patientID)
    val t2med = medication.filter(x => type2_dm_med.contains(x.medicine)).map(x => x.patientID)
    val pass_diag = diagnostic.map(x => x.patientID).subtract(t1dx).intersection(t2dx)    
    val with_t1med = pass_diag.intersection(t1med)
    val without_t1med = pass_diag.subtract(t1med)
    val with_t2med = with_t1med.intersection(t2med)
    val without_t2med = with_t1med.subtract(t2med)
    val t1_date = medication.filter(x => type1_dm_med(x.medicine)).map(x => (x.patientID, x.date.getTime)).reduceByKey((x,y) => math.min(x,y))
    val t2_date = medication.filter(x => type2_dm_med(x.medicine)).map(x => (x.patientID, x.date.getTime)).reduceByKey((x,y) => math.min(x,y))
    val t2med_before_t1med_tmp = t1_date.join(t2_date).filter(x => x._2._1 >= x._2._2).map(x => x._1)
    val t2med_before_t1med= t2med_before_t1med_tmp.intersection(with_t2med)
    val casePatients = without_t1med.union(without_t2med).union(t2med_before_t1med).distinct().map(x => (x,1))
    // DEBUG
    // println("[COUNT casePatients] "+casePatients.count())
    // casePatients.take(5).foreach(println)


    /** Find CONTROL Patients */
    // val controlPatients = sc.parallelize(Seq(("controlPatients-one", 2), ("controlPatients-two", 2), ("controlPatients-three", 2)))
    val glucose = labResult.filter(x => x.testName.contains("glucose")).map(x => x.patientID).distinct()
    val abnormal = labResult.filter{x =>
      val name = x.testName
      val value = x.value
      ((name=="hba1c" && value>=6.0) || (name=="hemoglobin a1c" && value>=6.0) || (name=="fasting glucose" && value>=110)|| (name=="fasting blood glucose" && value>=110)|| (name=="fasting plasma glucose" && value>=110)|| (name=="glucose" && value>110)|| (name=="glucose, serum" && value>110))
    }.map(x => x.patientID)
    val related_dx = diagnostic.filter(x => dm_related_dx.contains(x.code) || x.code.contains("250")).map(x => x.patientID)
    val controlPatients = glucose.subtract(abnormal).subtract(related_dx).distinct().map(x => (x,2))
    // DEBUG
    // println("[COUNT controlPatients] "+controlPatients.count())
    // controlPatients.take(5).foreach(println)


    /** Find OTHER Patients */
    // val others = sc.parallelize(Seq(("others-one", 3), ("others-two", 3), ("others-three", 3)))
    val allPatients = sc.union(diagnostic.map(_.patientID),medication.map(_.patientID),labResult.map(_.patientID)).distinct()
    val others = allPatients.subtract(casePatients.map(x => x._1)).subtract(controlPatients.map(x => x._1)).map(x => (x,3))
    // DEBUG
    // println("[COUNT others] "+others.count())
    // others.take(5).foreach(println)


    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)
    //DEBUG
    // println("[COUNT phenotypeLabel] "+phenotypeLabel.count())
    // phenotypeLabel.take(5).foreach(println)

    /** Return */
    phenotypeLabel
  }
}
