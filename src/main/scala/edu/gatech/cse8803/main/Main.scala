package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler


object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    //val (patient, medication, labResult, diagnostic) = loadRddRawData(sqlContext)

    println("--------------test-----------")
    
    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientProperty], RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    List("data/PATIENT.csv", "data/LAB.csv", "data/DIAGNOSTIC.csv", "data/MEDICATION.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patient = sqlContext.sql(
      """
        |SELECT subject_id, sex, dob, dod
        |FROM PATIENT
      """.stripMargin)
      .map(r => PatientProperty(r(0).toString, r(1).toString, r(2).toString, r(3).toString ))

    val labResult = sqlContext.sql(
      """
        |SELECT subject_id, date, lab_name, value
        |FROM LAB
        |WHERE value IS NOT NULL and value <> ''
      """.stripMargin)
      .map(r => LabResult(r(0).toString, r(1).toString.toLong, r(2).toString, r(3).toString ))

    val diagnostic = sqlContext.sql(
      """
        |SELECT subject_id, date, code, sequence
        |FROM DIAGNOSTIC
      """.stripMargin)
      .map(r => Diagnostic(r(0).toString, r(1).toString.toLong, r(2).toString, r(3).toString.toInt ))

    val medication = sqlContext.sql(
      """
        |SELECT subject_id, date, med_name
        |FROM MEDICATION
      """.stripMargin)
      .map(r => Medication(r(0).toString, r(1).toString.toLong, r(2).toString))

    (patient, medication, labResult, diagnostic)

  }


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
