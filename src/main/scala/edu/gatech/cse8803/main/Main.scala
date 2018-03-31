package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.features.FeatureConstruction

import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Date
import java.text.SimpleDateFormat


object Main {
  def main(args: Array[String]) {
	/*  CONFIGURATION  START  */
	
	//location of MIMIC and other input files
	val dataDir = "data"
	//location of the program generated features file(s)
	val featureDir = "src/main/scala/edu/gatech/cse8803/output_features"
	//location of the output files generated by this
	val outputDir = "src/main/scala/edu/gatech/cse8803/output"
	//location of Prinston stopword file
	val stopWordFileLocation = "princeton_stopwords.txt"
	
	println("--------------config-----------")
	println(dataDir)
	println(featureDir)
	println(outputDir)
	println(stopWordFileLocation)
	println("--------------config-----------")
	
	/*  CONFIGURATION  END  */
	
	
	
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
	
    val sc = createContext
    val sqlContext = new SQLContext(sc)
	

    /** initialize loading of data */
	val (patient, diagnostics, medications, labResults, notes, comorbidities, icustays, saps) = loadRddRawData(sqlContext, dataDir)

	//create features from data in dataDir and output to featureDir
	//createFeatures(spark, dataDir, featureDir, stopWordFileLocation)
	
	/** feature construction with base features */
    val baseFeatures = FeatureConstruction.constructBaseFeatureTuple(sc, patient, saps)
	print("------------ Base Features -----------")
	baseFeatures.foreach(println)
    print("------------ Base Features End -----------")
	
	
	
    //run models on created features
	//runMultiModels
		
    sc.stop()
  }
  
  
  def loadRddRawData(sqlContext: SQLContext, inputPath: String): (RDD[Patient], RDD[Diagnostic], RDD[Medication], RDD[LabResult], RDD[Note], RDD[Comorbidities], RDD[IcuStays], RDD[Saps2]) = {
  
	println(s"---------- Loading RDD Raw Data ----------")
  
	val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
	
	
	
	/*    PATIENTS    */
	
	List(inputPath + "/PATIENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "PATIENTS"))

    val patient = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, GENDER, DOB, DOD, EXPIRE_FLAG
        |FROM PATIENTS
      """.stripMargin)
      .map(r => Patient(
        r(0).toString,
        if (r(1).toString.toLowerCase=="m") true else false,
        new Date(dateFormat.parse(r(2).toString).getTime),
        r(4).toString.toDouble,
        if (r(4).toString.toInt == 1 && r(3).toString.trim != "")
          new Date(dateFormat.parse(r(3).toString).getTime)
        else
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime),
        new Date(dateFormat.parse("1971-01-01 00:00:00").getTime),
        0.0
      ))	
	//TODO: patient age is always 0.0
	//patient.foreach(println)
    println(s"Patient count: ${patient.count}")
	
	
	
	/*     DIAGNOSTICS     */
	
	List(inputPath + "/DIAGNOSES_ICD.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "DIAGNOSES_ICD"))

    val diagnostics = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, ICD9_CODE, SEQ_NUM
        |FROM DIAGNOSES_ICD
      """.stripMargin)
      .map(r => Diagnostic(
        r(0).toString,
        r(1).toString,
        new Date(dateFormat.parse("1900-01-01 00:00:00").getTime),
        r(2).toString,
        if (r(3).toString.isEmpty) 0 else r(3).toString.toInt
      ))	
	
	//diagnostics.foreach(println)
    println(s"Diagonstics count: ${diagnostics.count}")
	
	
	
	/*     MEDICATIONS     */
	
	List(inputPath + "/PRESCRIPTIONS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "PRESCRIPTIONS"))

    val medications = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, STARTDATE, ENDDATE //, DRUG
        |FROM PRESCRIPTIONS
      """.stripMargin)
      .map(r => Medication(
        r(0).toString,
        r(1).toString,
        if (!r(2).toString.isEmpty)
          new Date(dateFormat.parse(r(2).toString).getTime)
        else if (!r(3).toString.isEmpty)
          new Date(dateFormat.parse(r(3).toString).getTime)
        else
          new Date(dateFormat.parse("1900-01-01 00:00:00").getTime),
        r(3).toString
      ))
	
	//medications.foreach(println)
    println(s"Medication count: ${medications.count}")
	
	
	
	
	/*     LAB RESULT     */
	
	List(inputPath + "/LABEVENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "LABEVENTS"))

    val labResults = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, CHARTTIME, ITEMID, VALUENUM
        |FROM LABEVENTS
      """.stripMargin)
      .map(r => LabResult(
        r(0).toString,
        r(1).toString,
        new Date(dateFormat.parse(r(2).toString).getTime),
        r(3).toString,
        if (!r(4).toString.isEmpty) r(4).toString.toDouble else 0.0
      ))
	
	//labResults.foreach(println)
    println(s"Lab Result count: ${labResults.count}")
	
	
	
	
	/*     NOTES    */
	
	List(inputPath + "/NOTEEVENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "NOTEEVENTS"))

    val notes = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, CHARTDATE, CHARTTIME, CATEGORY, DESCRIPTION, TEXT
        |FROM NOTEEVENTS
      """.stripMargin)
	  .map(r => r)
      .filter(r => !r(4).toString.toLowerCase.contains("discharge summary"))
      .map(r => Note(
        r(0).toString,
        r(1).toString,
        if (r(3).toString.isEmpty) {
          val dateString = if (r(2).toString.trim.length == 10) r(2).toString.trim + " 00:00:00" else r(2).toString.trim
          new Date(dateFormat.parse(dateString).getTime)
        }
        else {
          val dateString = if (r(3).toString.trim.length == 10) r(3).toString.trim + " 00:00:00" else r(3).toString.trim
          new Date(dateFormat.parse(dateString).getTime) //dateFormat.parse(r(1).toString),
        },
        r(4).toString,
        r(5).toString,
        r(6).toString
      ))
	
	//notes.foreach(println)
    println(s"Notes count: ${notes.count}")
	
	
	
	
	
	/*     COMORBIDITIES    */
	
	List(inputPath + "/EHCOMORBIDITIES.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "EHCOMORBIDITIES"))

	//need to figure out 
	//class can not have that many params
    val comorbidities = null
	
	//comorbidities.foreach(println)
    //println(s"Comorbidities count: ${comorbidities.count}")
	
	
	
	
	/*     ICU STAYS    */
	
	List(inputPath + "/ICUSTAYS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "ICUSTAYS"))

    val icustays = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, INTIME, OUTTIME
        |FROM ICUSTAYS
      """.stripMargin)
      .map(r => IcuStays(
        r(0).toString,
        if (r(1).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(r(1).toString).getTime),
        if (r(2).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(r(2).toString).getTime)
      ))
	
	//icustays.foreach(println)
    println(s"ICU stays count: ${icustays.count}")
	
	
	
	
	/*     SAPS    */
	
	List(inputPath + "/SAPSII.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "SAPSII"))

    val saps = sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, icustay_id, sapsii, sapsii_prob, age_score,hr_score,sysbp_score,temp_score,        pao2fio2_score,uo_score,bun_score,wbc_score,potassium_score,sodium_score,bicarbonate_score,
        bilirubin_score,gcs_score,comorbidity_score,admissiontype_score
        |FROM SAPSII
      """.stripMargin)
      .map(r => Saps2(
        r(0).toString,
        r(1).toString,
        r(2).toString,
        if (r(3).toString.isEmpty) 0.0 else r(3).toString.toDouble,
        if (r(4).toString.isEmpty) 0.0 else r(4).toString.toDouble,
        if (r(5).toString.isEmpty) 0.0 else r(5).toString.toDouble,
        if (r(6).toString.isEmpty) 0.0 else r(6).toString.toDouble,
        if (r(7).toString.isEmpty) 0.0 else r(7).toString.toDouble,
        if (r(8).toString.isEmpty) 0.0 else r(8).toString.toDouble,
        if (r(9).toString.isEmpty) 0.0 else r(9).toString.toDouble,
        if (r(10).toString.isEmpty) 0.0 else r(10).toString.toDouble,
        if (r(11).toString.isEmpty) 0.0 else r(11).toString.toDouble,
        if (r(12).toString.isEmpty) 0.0 else r(12).toString.toDouble,
        if (r(13).toString.isEmpty) 0.0 else r(13).toString.toDouble,
        if (r(14).toString.isEmpty) 0.0 else r(14).toString.toDouble,
        if (r(15).toString.isEmpty) 0.0 else r(15).toString.toDouble,
        if (r(16).toString.isEmpty) 0.0 else r(16).toString.toDouble,
        if (r(17).toString.isEmpty) 0.0 else r(17).toString.toDouble,
        if (r(18).toString.isEmpty) 0.0 else r(18).toString.toDouble,
        if (r(19).toString.isEmpty) 0.0 else r(19).toString.toDouble
      ))
	
	//saps.foreach(println)
    println(s"SAPS count: ${saps.count}")
	
	
	
	println(s"---------- Done Loading RDD Raw Data ----------")
	
	
	//(null , null, null, null, null, null, null, null)
	(patient, diagnostics, medications, labResults, notes, comorbidities, icustays, saps)
  }
  
  
  
  


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Mortality Prediction Project", "local")
}
