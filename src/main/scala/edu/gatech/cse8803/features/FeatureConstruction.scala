package edu.gatech.cse8803.features

import java.sql.Date

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  type FeatureArrayTuple = (String, Array[Double])
  type FeatureTuple = ((String, String), Double)
  type LabelTuple = (String, Int)

  type FirstNoteInfo = (String, (Date, Boolean)) // Boolean = from note? (or from ICU inDate)

  abstract class MortalityType()
  case class InICU() extends MortalityType
  case class In30Days() extends MortalityType
  case class In1Year() extends MortalityType

  /* Construct 3 baseline features: age, sex, SAPS II score */
  def constructBaselineFeatureTuples(sc: SparkContext, patients: RDD[Patient],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2]): RDD[FeatureTuple] = {

    // Join with icuStays for RDD[((patientID, hadmID, icuStayID), Patient)]
    val patientsWithIcu: RDD[((String, String, String), Patient)] = icuStays.keyBy(_.patientID)
      .join(patients.keyBy(_.patientID))
      .map{ case(pid, (icu, p)) => {
        ((p.patientID, icu.hadmID, icu.icuStayID), p)
      } }

    val values = patientsWithIcu
      .join(saps2s.keyBy(s => (s.patientID, s.hadmID, s.icuStayID)))
      .map{ case((pid, hadmID, icuStayID), (p, s)) => (pid, (p.age, p.isMale, s.score)) }

    val ages = values.map{ case(pid, vals) => ((pid, "patient_age"), vals._1) }
    val sexes = values.map{ case(pid, vals) => ((pid, "patient_sex"), vals._2.toDouble) }
    val scores = values.map{ case(pid, vals) => ((pid, "saps_score"), vals._3) }

    sc.union(ages, sexes, scores)
  }

  /* Construct 3 baseline features: age, sex, SAPS II score */
  def constructBaselineFeatureTuples(sc: SparkContext, patients: RDD[Patient],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2], firstNoteDates: RDD[FirstNoteInfo],
      hours: Int): RDD[FeatureTuple] = {

    val (filteredPatients, filteredIcuStays) = filterDataOnHoursSinceFirstNote(
      patients, icuStays, firstNoteDates, hours)

    constructBaselineFeatureTuples(sc, filteredPatients, filteredIcuStays, saps2s)
  }

  /* Construct 3 baseline features: age, sex, SAPS II score */
  def constructBaselineFeatureArrayTuples(sc: SparkContext, patients: RDD[Patient],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2]): RDD[FeatureArrayTuple] = {

    // Join with icuStays for RDD[((patientID, hadmID, icuStayID), Patient)]
    val patientsWithIcu: RDD[((String, String, String), Patient)] = icuStays.keyBy(_.patientID)
      .join(patients.keyBy(_.patientID))
      .map{ case(pid, (icu, p)) => {
        ((p.patientID, icu.hadmID, icu.icuStayID), p)
      } }

    val values = patientsWithIcu
      .join(saps2s.keyBy(s => (s.patientID, s.hadmID, s.icuStayID)))
      .map({ case((pid, hadmID, icuStayID), (p, s)) => {
          (pid, (p.age, p.isMale.toDouble, s.score))
        }
      })

    values.map(x => (x._1, Array[Double](x._2._1, x._2._2, x._2._3)))
  }

  /* Construct 3 baseline features: age, sex, SAPS II score */
  def constructBaselineFeatureArrayTuples(sc: SparkContext, patients: RDD[Patient],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2], firstNoteDates: RDD[FirstNoteInfo],
      hours: Int): RDD[FeatureArrayTuple] = {

    val (filteredPatients, filteredIcuStays) = filterDataOnHoursSinceFirstNote(
      patients, icuStays, firstNoteDates, hours)

    constructBaselineFeatureArrayTuples(sc, filteredPatients, filteredIcuStays, saps2s)
  }

  def generateLabelTuples(patients: RDD[Patient], icuStays: RDD[IcuStay],
      mortType: MortalityType): RDD[LabelTuple] = {

    val MILLISECONDS_IN_DAY = 24L * 60 * 60 * 1000

    val tuples = patients.keyBy(_.patientID).join(icuStays.keyBy(_.patientID))
      .map{ case(pid, (p, icu)) => {
          val d = p.dod.getTime
          val outd = icu.outDate.getTime
          val diedInThisPeriod = mortType match {
            case InICU() => d >= icu.inDate.getTime && d <= outd
            case In30Days() => d > outd &&
              ((d - outd) / MILLISECONDS_IN_DAY) <= 30
            case In1Year() => d > outd &&
              ((d - outd) / MILLISECONDS_IN_DAY) > 30 &&
              ((d - outd) / MILLISECONDS_IN_DAY) <= 365
          }
          if (diedInThisPeriod) (pid, 1)
          else (pid, 0)
        }
      }

    tuples
  }

  /* Filter for patients and icuStays remaining in ICU x hours after their first note */
  def filterDataOnHoursSinceFirstNote(patients: RDD[Patient], icuStays: RDD[IcuStay],
      firstNoteDates: RDD[FirstNoteInfo], hours: Int): (RDD[Patient], RDD[IcuStay]) = {
    if (hours <= 0) return (patients, icuStays)

    val MILLISECONDS_IN_HOUR = 60L * 60 * 1000
    val joined = patients.keyBy(_.patientID).join(icuStays.keyBy(_.patientID))
      .join(firstNoteDates)
      .filter{ case(pid, ((p, icu), (date, b))) => {
          val bound = date.getTime + MILLISECONDS_IN_HOUR*hours
          (icu.outDate.getTime > bound && p.dod.getTime > bound)
        }
      }

    (joined.map(x => x._2._1._1), joined.map(x => x._2._1._2))
  }

  /* Filter for patients and icuStays remaining in ICU x hours after their first note */
  def filterDataOnHoursSinceFirstNote(patients: RDD[Patient], icuStays: RDD[IcuStay], notes: RDD[Note],
      firstNoteDates: RDD[FirstNoteInfo], hours: Int): (RDD[Patient], RDD[IcuStay], RDD[Note]) = {
    if (hours <= 0) return (patients, icuStays, notes)

    val (fPats, fIcus) = filterDataOnHoursSinceFirstNote(patients, icuStays, firstNoteDates, hours)

    val MILLISECONDS_IN_HOUR = 60L * 60 * 1000
    val filteredNotes = firstNoteDates.join(notes.keyBy(_.patientID))
      .filter{ case(pid, ((date, b), note)) => {
          val bound = date.getTime + MILLISECONDS_IN_HOUR*hours
          note.chartDate.getTime <= bound
        }
      }
      .map{ case(pid, ((date, b), note)) => note }

    (fPats, fIcus, filteredNotes)
  }

  /**
   * Gets patients with IcuStay and calculates their age in years at admission.
   * As a result, patients without an IcuStay and IcuStays without patients are
   * filtered out.
   */
  def processRawPatientsAndIcuStays(patients: RDD[Patient], icuStays: RDD[IcuStay]): (RDD[Patient], RDD[IcuStay]) = {
    // NOTE: it would be better to use java.util.Calendar to calculate differences
    val MILLISECONDS_IN_DAY = 24L * 60 * 60 * 1000
    val MILLISECONDS_IN_YEAR = MILLISECONDS_IN_DAY * 365L

    // Get most recent IcuStay for each patient in icuStays RDD
    val uniqueIcuStays = icuStays
      .keyBy(_.patientID)
      .reduceByKey((x, y) => if (x.outDate.getTime > y.outDate.getTime) x else y)
      .map(x => x._2)

    // s
    val adjPatients = uniqueIcuStays.keyBy(_.patientID).join(patients.keyBy(_.patientID))
      .map{ case(pid, (icu, p)) => {
        val age = (icu.inDate.getTime - p.dob.getTime).toDouble / MILLISECONDS_IN_YEAR
        if (age >= 300) {
          val dontknow = 89.0 // MIMIC III makes anyone older than 89 be 300 at their first admission
          Patient(p.patientID, p.isMale, p.dob, p.isDead, p.dod, p.indexDate, dontknow)
        } else {
          Patient(p.patientID, p.isMale, p.dob, p.isDead, p.dod, p.indexDate, age)
        }
      } }

    (adjPatients, uniqueIcuStays)
  }

  /**
   * Return patientID and first note Date pairs. Assuming IcuStays with unique patientIDs!
   *
   * NOTE: Some patients have 0 notes, so the first note time is set to their inDate.
  */
  def processNotesAndCalculateStartDates(patients: RDD[Patient], icuStays: RDD[IcuStay],
      notes: RDD[Note]): (RDD[Note], RDD[FirstNoteInfo]) = {

    val patIcu: RDD[((String, String), IcuStay)] = icuStays.keyBy(_.patientID)
      .join(patients.keyBy(_.patientID))
      .map{ case(pid, (icu, p)) => ((pid, icu.hadmID), icu) }

    // Filter for ntoes that are within the inDate and outDate of IcuStays
    val notesInIcu: RDD[((String, String), Note)] = patIcu
      .join(notes.keyBy(x => (x.patientID, x.hadmID)))
      .filter{ case((pid, hadmID), (icu, note)) => {
          val noteTime = note.chartDate.getTime
          (icu.inDate.getTime <= noteTime
              && noteTime <= icu.outDate.getTime)
        }
      }
      .map{ case((pid, hadmID), (icu, note)) => ((pid, hadmID), note) }

    println(s"notesInIcu count: ${notesInIcu.count}")

    val adjustedNotes: RDD[Note] = notesInIcu
      .map{ case((pid, hadmID), note) => note } // Just the notes

    println(s"adjustedNotes count: ${adjustedNotes.count}")

    val firstNoteDates = notesInIcu
      .map{ case((pid, hadmID), note) => (pid, note.chartDate) }
      .reduceByKey((d1, d2) => if (d1.getTime < d2.getTime) d1 else d2)

    val startDates = icuStays.keyBy(_.patientID)
      .leftOuterJoin(firstNoteDates)
      .map{ case(pid, (icu, dateOption)) => {
          if (dateOption.isEmpty) {
            (pid, (icu.inDate, false))
          } else {
            (pid, (dateOption.get, true))
          }
        }
      }

    (adjustedNotes, startDates)
  }

  def constructForSVM(features: RDD[FeatureArrayTuple], labels: RDD[LabelTuple]): RDD[LabeledPoint] = {
    val points = labels.join(features)
      .map{ case(pid, (label, fArr)) => new LabeledPoint(label.toDouble, Vectors.dense(fArr)) }

    points
  }
}
