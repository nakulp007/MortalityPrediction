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

  type FirstNoteInfo = (String, Date) // Boolean = from note? (or from ICU inDate)

  abstract class MortalityType()
  case class InICU() extends MortalityType
  case class In30Days() extends MortalityType
  case class In1Year() extends MortalityType

  val maxPossibleSaps2 = 163
  val maxPossibleAge = 89

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

    val ages = values.map{ case(pid, vals) => ((pid, "patient_age"), vals._1 / maxPossibleAge) }
    val sexes = values.map{ case(pid, vals) => ((pid, "patient_sex"), vals._2.toDouble) }
    val scores = values.map{ case(pid, vals) => ((pid, "saps_score"), vals._3 / maxPossibleSaps2) }

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
          (pid, (p.age / maxPossibleAge, p.isMale.toDouble, s.score / maxPossibleSaps2))
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
              //((d - outd) / MILLISECONDS_IN_DAY) > 30 &&
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
    val offsetInMilliseconds = MILLISECONDS_IN_HOUR*hours

    val joined = patients.keyBy(_.patientID).join(icuStays.keyBy(_.patientID))
      .join(firstNoteDates)
      .filter{ case(pid, ((p, icu), date)) => {
          val bound = date.getTime + offsetInMilliseconds
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
    val offsetInMilliseconds = MILLISECONDS_IN_HOUR*hours

    val filteredNotes = firstNoteDates.join(notes.keyBy(_.patientID))
      .filter{ case(pid, (date, note)) => {
          val bound = date.getTime + offsetInMilliseconds
          note.chartDate.getTime <= bound
        }
      }
      .map{ case(pid, (date, note)) => note }

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


    val icuPatientPairs = uniqueIcuStays.keyBy(_.patientID).join(patients.keyBy(_.patientID))
      .map{ case(pid, (icu, p)) => {
        val age = (icu.inDate.getTime - p.dob.getTime).toDouble / MILLISECONDS_IN_YEAR
        if (age >= 300) {
          val dontknow = 89.0 // MIMIC III makes anyone older than 89 be 300 at their first admission
          (icu, Patient(p.patientID, p.isMale, p.dob, p.isDead, p.dod, p.indexDate, dontknow))
        } else {
          (icu, Patient(p.patientID, p.isMale, p.dob, p.isDead, p.dod, p.indexDate, age))
        }
      } }
      .filter(x => x._2.age >= 18)

    val adjPatients = icuPatientPairs.map(x => x._2)
    val adjIcuStays = icuPatientPairs.map(x => x._1)



    (adjPatients, adjIcuStays)
  }

  /**
   * Return patientID and first note Date pairs. Assuming IcuStays with unique patientIDs!
   * Filter out patients with less than a total of 100 stopwords.
   * Filter out notes that may have occurred after the end of the day in which
   * patient died or was discharged.
  */
  def processNotesAndCalculateStartDates(patients: RDD[Patient], icuStays: RDD[IcuStay],
      notes: RDD[Note], stopwords: Set[String]):
      (RDD[Patient], RDD[IcuStay], RDD[Note], RDD[FirstNoteInfo]) = {
    val sc = patients.context

    val patIcu: RDD[((String, String), IcuStay)] = icuStays.keyBy(_.patientID)
      .join(patients.keyBy(_.patientID))
      .map{ case(pid, (icu, p)) => ((pid, icu.hadmID), icu) }

    // Filter for notes that are within the inDate and outDate of IcuStays
    // This also automatically filters out patients without HADM_ID even if
    // they are not during data loading.
    val notesInIcu: RDD[(String, Note)] = patIcu
      .join(notes.keyBy(x => (x.patientID, x.hadmID)))
      .filter{ case((pid, hadmID), (icu, note)) => {
          val noteTime = note.chartDate.getTime
          (icu.inDate.getTime <= noteTime
              && noteTime < icu.outDate.getTime)
        }
      }
      .map{ case((pid, hadmID), (icu, note)) => (pid, note) }

    println(s"notesInIcu count: ${notesInIcu.count}")

    // Count the number of non-stopwords for each patient
    // Then filter for those who have at least 100 words
    val broadcastStopwords = sc.broadcast(stopwords)
    val filteredPatientWordCounts = notesInIcu
      .map{ case(pid, note) => {
        val count = filterSpecialCharacters(note.text)
          .toLowerCase
          .split("\\s")
          .filter(_.length > 3)
          .filter(_.forall(java.lang.Character.isLetter))
          .filter(!broadcastStopwords.value.contains(_))
          .size
        (pid, count)
        }
      }
      .reduceByKey(_ + _)
      .filter(_._2 >= 100)

    val filteredNotesInIcu = notesInIcu
      .join(filteredPatientWordCounts)
      .map{ case(pid, (note, count)) => (pid, note) }

    val adjustedNotes: RDD[Note] = filteredNotesInIcu
      .map{ case(pid, note) => note } // Just the notes

    val firstNoteDates = filteredNotesInIcu
      .map{ case(pid, note) => (pid, note.chartDate) }
      .reduceByKey((d1, d2) => if (d1.getTime < d2.getTime) d1 else d2)

    val adjustedPatients = firstNoteDates
      .join(patients.keyBy(_.patientID))
      .map{ case(pid, (date, p)) => p }

    val adjustedIcuStays = firstNoteDates
      .join(icuStays.keyBy(_.patientID))
      .map{ case(pid, (date, icu)) => icu }

    (adjustedPatients, adjustedIcuStays, adjustedNotes, firstNoteDates)
  }

  // Using the fact that firstNoteDates only have patietnIDs with a first note.
  def filterOutPatientIcuWithoutFirstNote(patients: RDD[Patient], icuStays: RDD[IcuStay],
      firstNoteDates: RDD[FirstNoteInfo]): (RDD[Patient], RDD[IcuStay]) = {
    val pairs = patients.keyBy(_.patientID).join(icuStays.keyBy(_.patientID))
      .join(firstNoteDates)

    val filteredPatients = pairs.map{ case(pid, ((p, icu), firstNoteDate)) => p }
    val filteredIcuStays = pairs.map{ case(pid, ((p, icu), firstNoteDate)) => icu }

    (filteredPatients, filteredIcuStays)
  }

  def retrospectiveTopicModel(notes: RDD[Note], stopWords: Set[String], numIterations: Int=200, k: Int=50) : RDD[FeatureArrayTuple] = {
    val sc = notes.context

    val filteredNotes  = notes.map(x => (x.patientID, filterSpecialCharacters(x.text)))
      .reduceByKey((x, y) => x + " " + y)

    println(s"Filtered notes count: ${filteredNotes.count}")

    // create index map for patients
    //val patientsDocsMap = filteredNotes.map(x => x._1).collect().zipWithIndex.toMap
    //patientsDocsMap.take(5).foreach(println)

    val docPatientMap = filteredNotes.map(x => x._1)
      .zipWithIndex
      .map(x => (x._2, x._1))
      .collectAsMap

    // create corpus from entire text
    val corpus: RDD[String] = filteredNotes.map(x => x._2)

    //  tokenize word counts
    val tokenized: RDD[Seq[String]] = corpus.map(_.toLowerCase.split("\\s"))
      .map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

    val broadcastStopWords = sc.broadcast(stopWords)
    // create vocabularay and remove stop words as well
    val vocabArray: Array[String] = tokenized
      .flatMap(_.map(_ -> 1L))
      .reduceByKey(_ + _)
      .filter(word => !broadcastStopWords.value.contains(word._1))
      .map(_._1).collect()
    broadcastStopWords.unpersist

    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
    val vocabSize = vocab.size
    val broadcastVocab = sc.broadcast(vocab)

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] = tokenized.zipWithIndex
      .map { case (tokens, id) => {
          val counts = new scala.collection.mutable.HashMap[Int, Double]()
          tokens.foreach { term =>
              if (broadcastVocab.value.contains(term)) {
                val idx = broadcastVocab.value(term)
                counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
              }
          }
          (id, Vectors.sparse(broadcastVocab.value.size, counts.toSeq))
        }
      }
    broadcastVocab.unpersist

    //documents.take(5).foreach(println)

    // run LDA model
    val lda = new LDA()
      .setK(k)
      .setMaxIterations(numIterations)
      .setBeta(200.0 / vocabSize + 1) // Following Ghassemi, but +1 is per library's recommendation
      //.setAlpha(50.0 / k) // Ghassemi uses this, but library recommends the default = (50/k)+1

    val ldaModel = lda.run(documents)

    val broadcastVocabArray = sc.broadcast(vocabArray)
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
        terms.zip(termWeights).map {
          case (term, weight) => (broadcastVocabArray.value(term.toInt), weight)
        }
    }
    broadcastVocabArray.unpersist

    val distLdaModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val topTopicsForDoc = distLdaModel.topicDistributions
    //topTopicsForDoc.take(5).foreach(println)

    println(s"documents-Count: ${topTopicsForDoc.count}")

    println(s"topics-Type: ${topTopicsForDoc.getClass}")

    val broadcastDocPatientMap = sc.broadcast(docPatientMap)
    val featuresRdd = topTopicsForDoc.map(row => (broadcastDocPatientMap.value(row._1.toInt), row._2))
    broadcastDocPatientMap.unpersist

    val finalNoteFeatures = featuresRdd.map(x => (x._1, x._2.toArray))

    println(s"final-RDD type : ${featuresRdd.getClass}")
    println(s"Number of note features: ${finalNoteFeatures.count}")

    (finalNoteFeatures)
  }

  def constructDerivedFeatures( comorbidities: RDD[Comorbidities]): RDD[FeatureArrayTuple] = {
    

    val comorbiditiesUnique = comorbidities
      .map(x => ((x.patientID, x.hadmID), x))
      .reduceByKey((x, y) => if (x.hadmID.toDouble > y.hadmID.toDouble) x else y)
      .map(_._2)
      .map(x => (x.patientID, x))
      .reduceByKey((x, y) => if (x.hadmID.toDouble > y.hadmID.toDouble) x else y)
      .map(_._2)

      
      val comorbiditiesFeatures = comorbiditiesUnique.map(x => (x.patientID, Array[Double](x.allValues.charAt(0).toDouble, x.allValues.charAt(1).toDouble, x.allValues.charAt(2).toDouble,
        x.allValues.charAt(3).toDouble,x.allValues.charAt(4).toDouble,x.allValues.charAt(5).toDouble,x.allValues.charAt(6).toDouble,x.allValues.charAt(7).toDouble,
        x.allValues.charAt(8).toDouble,x.allValues.charAt(9).toDouble,x.allValues.charAt(10).toDouble,x.allValues.charAt(11).toDouble,x.allValues.charAt(12).toDouble,
        x.allValues.charAt(13).toDouble,x.allValues.charAt(14).toDouble,x.allValues.charAt(15).toDouble,x.allValues.charAt(16).toDouble,x.allValues.charAt(17).toDouble,
        x.allValues.charAt(18).toDouble,x.allValues.charAt(19).toDouble,x.allValues.charAt(20).toDouble,x.allValues.charAt(21).toDouble,x.allValues.charAt(22).toDouble,
        x.allValues.charAt(23).toDouble,x.allValues.charAt(24).toDouble,x.allValues.charAt(25).toDouble,x.allValues.charAt(26).toDouble,x.allValues.charAt(27).toDouble,
       x.allValues.charAt(28).toDouble,x.allValues.charAt(29).toDouble)))


      comorbiditiesFeatures


  }

  def filterSpecialCharacters(document: String) = {

    document.replaceAll( """[! @ # $ % ^ & * ( ) \[ \] . \\ / _ { } + - − , " ' ~ ; : ` ? = > < --]""", " ")
      //.replaceAll("""\.\s""", " ")
      .replaceAll("""\w*\d\w*""", " ")        //replace digits
      .replaceAll("""\s[A-Z,a-z]\s""", " ")   //replace single digit char
      .replaceAll("""\s[A-Z,a-z]\s""", " ")   //replace consecutive single digit char
  }

  def constructForSVM(features: RDD[FeatureArrayTuple], labels: RDD[LabelTuple]): RDD[LabeledPoint] = {
    val points = labels.join(features)
      .map{ case(pid, (label, fArr)) => new LabeledPoint(label.toDouble, Vectors.dense(fArr)) }

    points
  }

  def constructForSVMSparse(features: RDD[FeatureTuple], labels: RDD[LabelTuple]): RDD[LabeledPoint] = {
    features.cache

    /** create a feature name to id map*/
    val fmap = features
      .map(x => (x._1._2.toLowerCase, x._2))
      .aggregateByKey(0)(
        (u, d) => 0,
        (u1, u2) => 0)
      .zipWithIndex.map(x => (x._1._1, x._2))
      .collectAsMap

    val sc = features.context
    val broadcastFmap = sc.broadcast(fmap)

    /** transform input feature */
    val tf = features
      .map(x => (x._1._1, ((broadcastFmap.value get x._1._2.toLowerCase).get.toInt, x._2)))
      .groupByKey

    broadcastFmap.unpersist

    val result = tf.map(x => (x._1, Vectors.sparse(fmap.size, x._2.toSeq)))

    val points = result.join(labels)
      .map{ case((pid, (vector, label))) => new LabeledPoint(label.toDouble, vector) }

    points
  }


}
