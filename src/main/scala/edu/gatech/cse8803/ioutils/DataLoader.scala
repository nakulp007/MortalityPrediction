package edu.gatech.cse8803.ioutils

import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Date
import java.text.SimpleDateFormat

object DataLoader {
  def loadRddRawData(sqlContext: SQLContext, inputPath: String): (RDD[Patient], RDD[Diagnostic], RDD[Medication], RDD[LabResult], RDD[Note], RDD[Comorbidities], RDD[IcuStay], RDD[Saps2]) = {

    println("---------- Loading RDD Raw Data ----------")

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    /*    PATIENTS    */
    val patients = loadPatientRDD(sqlContext, inputPath, dateFormat)
    //patient.foreach(println)
    println(s"Patient count: ${patients.count}")


    /*     DIAGNOSTICS     */
    val diagnostics = null //loadDiagnosticRDD(sqlContext, inputPath, dateFormat)
    //diagnostics.foreach(println)
    //println(s"Diagonstics count: ${diagnostics.count}")


    /*     MEDICATIONS     */
    val medications = null //loadMedicationRDD(sqlContext, inputPath, dateFormat)
    //medications.foreach(println)
    //println(s"Medication count: ${medications.count}")


    /*     LAB RESULT     */
    val labResults = null //loadLabResultRDD(sqlContext, inputPath, dateFormat)
    //labResults.foreach(println)
    //println(s"Lab Result count: ${labResults.count}")


    /*     NOTES    */
    val notes = loadNoteRDD(sqlContext, inputPath, dateFormat)
    //notes.foreach(println)
    //println(s"Notes count: ${notes.count}")


    /*     COMORBIDITIES    */
    //need to figure out
    //class can not have that many params
    val comorbidities = loadComorbiditiesRDD(sqlContext, inputPath, dateFormat)
    //comorbidities.foreach(println)
    //println(s"Comorbidities count: ${comorbidities.count}")


    /*     ICU STAYS    */
    val icuStays = loadIcuStayRDD(sqlContext, inputPath, dateFormat)
    //icuStays.foreach(println)
    println(s"ICU stays count: ${icuStays.count}")


    /*     SAPS    */
    val saps2s = loadSaps2RDD(sqlContext, inputPath, dateFormat)
    //saps.foreach(println)
    println(s"SAPS count: ${saps2s.count}")


    println(s"---------- Done Loading RDD Raw Data ----------")

    //(null , null, null, null, null, null, null, null)
    (patients, diagnostics, medications, labResults, notes, comorbidities, icuStays, saps2s)
  }

  def loadPatientRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Patient] = {
    List(inputPath + "/PATIENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "PATIENTS"))

    sqlContext.sql(
      """
        |SELECT SUBJECT_ID, GENDER, DOB, DOD, EXPIRE_FLAG
        |FROM PATIENTS
      """.stripMargin)
      .map(r => Patient(
        r(0).toString,
        if (r(1).toString.toLowerCase=="m") 1 else 0,
        new Date(dateFormat.parse(r(2).toString).getTime),
        r(4).toString.toInt,
        if (r(4).toString.toInt == 1 && r(3).toString.trim != "")
          new Date(dateFormat.parse(r(3).toString).getTime)
        else
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime),
        new Date(dateFormat.parse("1971-01-01 00:00:00").getTime),
        0.0
      ))
  }

  def loadDiagnosticRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Diagnostic] = {
    List(inputPath + "/DIAGNOSES_ICD.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "DIAGNOSES_ICD"))

    sqlContext.sql(
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
  }

  def loadMedicationRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Medication] = {
    List(inputPath + "/PRESCRIPTIONS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "PRESCRIPTIONS"))

    sqlContext.sql(
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
  }

  def loadLabResultRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[LabResult] = {
    List(inputPath + "/LABEVENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "LABEVENTS"))

    sqlContext.sql(
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
  }

	//subject_id, hadm_id, chartdate, charttime, category, description, text, storetime
	//SUBJECT_ID, HADM_ID, CHARTDATE, CHARTTIME, CATEGORY, DESCRIPTION, TEXT, STORETIME
  def loadNoteRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Note] = {
    List(inputPath + "/NOTEEVENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "NOTEEVENTS"))

    sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, CHARTDATE, CHARTTIME, CATEGORY, DESCRIPTION, TEXT, STORETIME
        |FROM NOTEEVENTS
      """.stripMargin)
    .map(r => r)
      .filter(r => !r(4).toString.toLowerCase.contains("discharge"))
      .filter(r => !r(1).toString.isEmpty) // outpatients and patients not admitted to ICU don't have HADM_ID
      .map(r => Note(
        r(0).toString,
        r(1).toString,
        if (r(3).toString.isEmpty) {
          if (r(7).toString.isEmpty) { // Check if STORETIME is available to use.
            val dateString = if (r(2).toString.trim.length == 10) r(2).toString.trim + " 00:00:00" else r(2).toString.trim
            new Date(dateFormat.parse(dateString).getTime)
          } else {
            val dateString = if (r(7).toString.trim.length == 10) r(7).toString.trim + " 00:00:00" else r(7).toString.trim
            new Date(dateFormat.parse(dateString).getTime)
          }
        } else {
          val dateString = if (r(3).toString.trim.length == 10) r(3).toString.trim + " 00:00:00" else r(3).toString.trim
          new Date(dateFormat.parse(dateString).getTime)
        },
        r(4).toString,
        r(5).toString,
        r(6).toString
      ))
  }

  def loadComorbiditiesRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Comorbidities] = {
    List(inputPath + "/EHCOMORBIDITIES.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "EHCOMORBIDITIES"))
    
    sqlContext.sql(
      """
        |SELECT subject_id,hadm_id,congestive_heart_failure,cardiac_arrhythmias,valvular_disease,
        |pulmonary_circulation,peripheral_vascular,hypertension,paralysis,other_neurological,
        |chronic_pulmonary,diabetes_uncomplicated,diabetes_complicated,hypothyroidism,renal_failure,
        |liver_disease,peptic_ulcer,aids,lymphoma,metastatic_cancer,solid_tumor,rheumatoid_arthritis,
        |coagulopathy,obesity,weight_loss,fluid_electrolyte,blood_loss_anemia,deficiency_anemias,
        |alcohol_abuse,drug_abuse,psychoses,depression
        |FROM EHCOMORBIDITIES
      """.stripMargin)
      .map(r => Comorbidities(
        r(0).toString,
        r(1).toString,
        (if (r(2).toString.isEmpty) "0" else r(2).toString) + 
        (if (r(3).toString.isEmpty) "0" else r(3).toString) + 
        (if (r(4).toString.isEmpty) "0" else r(4).toString) + 
        (if (r(5).toString.isEmpty) "0" else r(5).toString) + 
        (if (r(6).toString.isEmpty) "0" else r(6).toString)+ 
        (if (r(7).toString.isEmpty) "0" else r(7).toString)+ 
        (if (r(8).toString.isEmpty) "0" else r(8).toString)+ 
        (if (r(9).toString.isEmpty) "0" else r(9).toString)+ 
        (if (r(10).toString.isEmpty) "0" else r(10).toString) + 
        (if (r(11).toString.isEmpty) "0" else r(11).toString)+ 
        (if (r(12).toString.isEmpty) "0" else r(12).toString)+ 
        (if (r(13).toString.isEmpty) "0" else r(13).toString)+ 
        (if (r(14).toString.isEmpty) "0" else r(14).toString)+ 
        (if (r(15).toString.isEmpty) "0" else r(15).toString)+ 
        (if (r(16).toString.isEmpty) "0" else r(16).toString)+ 
        (if (r(17).toString.isEmpty) "0" else r(17).toString)+ 
        (if (r(18).toString.isEmpty) "0" else r(18).toString)+ 
        (if (r(19).toString.isEmpty) "0" else r(19).toString)+ 
        (if (r(20).toString.isEmpty) "0" else r(20).toString)+ 
        (if (r(21).toString.isEmpty) "0" else r(21).toString)+ 
        (if (r(22).toString.isEmpty) "0" else r(22).toString)+ 
        (if (r(23).toString.isEmpty) "0" else r(23).toString)+ 
        (if (r(24).toString.isEmpty) "0" else r(24).toString)+ 
        (if (r(25).toString.isEmpty) "0" else r(25).toString)+ 
        (if (r(26).toString.isEmpty) "0" else r(26).toString)+ 
        (if (r(27).toString.isEmpty) "0" else r(27).toString) + 
        (if (r(28).toString.isEmpty) "0" else r(28).toString)+ 
        (if (r(29).toString.isEmpty) "0" else r(29).toString)+ 
        (if (r(30).toString.isEmpty) "0" else r(30).toString)+ 
        (if (r(31).toString.isEmpty) "0" else r(31).toString)
        
      ))
  }

  def loadIcuStayRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[IcuStay] = {
    List(inputPath + "/ICUSTAYS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "ICUSTAYS"))

    sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, ICUSTAY_ID, INTIME, OUTTIME
        |FROM ICUSTAYS
      """.stripMargin)
      .map(r => IcuStay(
        r(0).toString,
        r(1).toString,
        r(2).toString,
        if (r(3).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(r(3).toString).getTime),
        if (r(4).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(r(4).toString).getTime)
      ))
  }

  def loadSaps2RDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Saps2] = {
    List(inputPath + "/SAPSII.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "SAPSII"))

    sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, icustay_id, sapsii, sapsii_prob, age_score,
          hr_score,sysbp_score,temp_score, pao2fio2_score,uo_score,bun_score,
          wbc_score,potassium_score,sodium_score,bicarbonate_score,
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
  }

}
