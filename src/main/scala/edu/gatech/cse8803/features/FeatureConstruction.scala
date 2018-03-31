package edu.gatech.cse8803.features

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  type FeatureTuple = ((String, String), Double)
  
  val obervationWindow = 2000L

  abstract class MortalityType()
  case class InICU12Hr() extends MortalityType
  case class InICU24Hr() extends MortalityType
  case class InICU48Hr() extends MortalityType
  case class InICU() extends MortalityType
  case class In30Days() extends MortalityType
  case class In1Year() extends MortalityType

  /*     Get base feature tuple     */
  def constructBaseFeatureTuple(sc: SparkContext, patient: RDD[Patient], saps: RDD[Saps2]): RDD[FeatureTuple] = {
	
	val patientMap = patient.map(x => (x.patientID, x.indexDate)).collectAsMap()
	//patientMap.foreach(println)
    
	val scPatientMap = sc.broadcast(patientMap)
	
	val sapsUnique = saps
      .filter(x => scPatientMap.value.contains(x.patientID))
      .map(x => ((x.patientID, x.hadmID), x))
      .reduceByKey((x, y) => if (x.icuStayID.toDouble > y.icuStayID.toDouble) x else y)
      .map(_._2)
      .map(x => (x.patientID, x))
      .reduceByKey((x, y) => if (x.hadmID.toDouble > y.hadmID.toDouble) x else y)
      .map(_._2)
    //sapsUnique.foreach(println)
	
	val sapsScore = sapsUnique.map(x => ((x.patientID, "saps_score"), x.sapsScore))
	//sapsScore.foreach(println)
	
	//println("---------------")
    val age = patient.map(x => ((x.patientID, "patient_age"), x.age))
	//age.foreach(println)
	
	//println("---------------")
    val sex = patient.map(x => ((x.patientID, "patient_sex"), if (x.isMale) 1.0 else 0.0))
	//sex.foreach(println)
	
	sc.union(age, sex, sapsScore)
  }
}


