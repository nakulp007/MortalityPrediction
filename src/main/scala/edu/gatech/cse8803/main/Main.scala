package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.{CSVUtils, DataLoader}
import edu.gatech.cse8803.features.FeatureConstruction._

import edu.gatech.cse8803.model._
import org.apache.spark.ml.param.{Param}
import org.apache.spark.ml.tuning.{CrossValidator}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Date
import java.text.SimpleDateFormat

object Main {
  def main(args: Array[String]) {
    /*  CONFIGURATION  START  */

    //location of MIMIC and other input files
    val dataDir = "data1000"
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

    val stopwords: Set[String] = sc.textFile(stopWordFileLocation).collect.toSet
    println(s"Stopwords count: ${stopwords.size}")

    /***************** initialize loading of data ********************/
    val (rawPatients, rawDiagnostics, rawMedications, rawLabResults,
      rawNotes, rawComorbidities, rawIcuStays, rawSaps2s) = DataLoader.loadRddRawData(sqlContext, dataDir)

    /***************** Process raw data beforehand! *******************/
    // Process for patients' age. Filters for most recent unique IcuStays for unique patients.
    val (patients1, icuStays1) = processRawPatientsAndIcuStays(rawPatients, rawIcuStays)
    println(s"Unique patient count: ${patients1.count}")
    println(s"Unique IcuStay count: ${icuStays1.count}")

    // Process notes and get start dates for each patient. Read comments for the function.
    val (patients, icuStays, notes, firstNoteDates) = processNotesAndCalculateStartDates(
      patients1, icuStays1, rawNotes, stopwords)

    val patientCount = patients.count
    val useExactSplits = patientCount <= 3000
    println(s"Patients with first note count: ${patientCount}")
    println(s"IcuStays with first note count: ${icuStays.count}")
    println(s"Notes after filtering: ${notes.count}")
    println(s"firstNoteDates count: ${firstNoteDates.count}")


    /************************* Generate labels ******************************/
    val labelsInIcu = generateLabelTuples(patients, icuStays, InICU())
    println(s"Num labels in ICU: ${labelsInIcu.count}")

    val labelsIn30Days = generateLabelTuples(patients, icuStays, In30Days())
    println(s"Num labels in 30 Day: ${labelsIn30Days.count}")

    val labelsIn1Year = generateLabelTuples(patients, icuStays, In1Year())
    println(s"Num labels in 1 Year: ${labelsIn1Year.count}")


    /****************** Baseline Feature Constructions **********************/
    /* Example of simple baseline feature construction with base features */
    //val baseFeatures = constructBaselineFeatureArrayTuples(
    //  sc, patients, icuStays, rawSaps2s)
    val baseFeatures = constructBaselineFeatureTuples(
      sc, patients, icuStays, rawSaps2s)
    println("------------ Base Features ------------")
    //baseFeatures.foreach(println)
    println(s"Baseline Feature Tuple count: ${baseFeatures.count}")
    println("------------ Base Features End ------------")


    /**************** Running baseline model based on hours **************/
    val (trainPatients, testPatients) = splitPatientIds(sc, patients, 0.7, useExactSplits)
    println(s"${trainPatients.count} train & ${testPatients.count} test patients")

    val labels = labelsInIcu // Change this to change the label type

    // One thing to watch out for from here on out is the fact that labels does not
    // get filtered or subsampled. The RDD is kept exactly the same!
    val subsampledTrainingPatients = subsampleTrainingPatients(
      trainPatients, labels, useExactSplits)
    println(s"${subsampledTrainingPatients.count} subsampled training patients")

    runBaseLineModel(sc, subsampledTrainingPatients, testPatients,
        icuStays, rawSaps2s, firstNoteDates, labels, useCv=false,
        numIterations=400,
        regParam=1)

    //runRetrospectiveTopicModel(sc, subsampledTrainingPatients, testPatients,
    //    notes, stopwords, firstNoteDates, labels, useCv=false,
    //    numIterations=300,
    //    regParam=0.1)

    sc.stop()
  }

  def splitPatientIds(sc: SparkContext, patients: RDD[Patient],
      trainProportion: Double, exact: Boolean): (RDD[Patient], RDD[Patient]) = {
    if (!exact) {
      val splits = patients.randomSplit(Array[Double](trainProportion, 1-trainProportion))
      (splits(0), splits(1))
    } else {
      val numToSample = (patients.count * 0.7).toInt
      val sample = sc.makeRDD(patients.takeSample(false, numToSample))
      (sample, patients.subtract(sample))
    }
  }

  // Adjusts the ratio of negative to positive examples to be 7:3.
  // Does nothing if the ratio is already smaller than that.
  def subsampleTrainingPatients(patients: RDD[Patient], labels: RDD[LabelTuple], exact: Boolean): RDD[Patient] = {
    val sc = labels.context

    // Count total number of training instances and postivie instances
    val labelsForThesePatients = patients.keyBy(_.patientID).join(labels)
      .map{ case(pid, (p, label)) => (pid, label) }
    val (numTraining, numTrainingPositive) = labelsForThesePatients
      .aggregate((0, 0))(
        (u, labelPair) => (u._1+1, u._2+labelPair._2),
        (u1, u2) => (u1._1+u2._1, u1._2+u2._2)
      )

    println(s"${numTrainingPositive} out of ${numTraining} training examples are positive.")

    // No need to subsample if the ratio is already high
    if (numTrainingPositive / numTraining.toDouble >= 0.3) {
      println("Negative to positive examples proportion is already 7:3 or smaller.")
      return patients
    }

    val positiveLabels = labelsForThesePatients.filter(_._2 == 1)
    val negativeLabels = labelsForThesePatients.filter(_._2 == 0)

    // We want to end up with a 7:3 ratio of negative to positive examples

    // Sample without replacement
    var sample = negativeLabels
    if (!exact) {
      val propToSample = (numTrainingPositive / 0.3 * 0.7) / (numTraining - numTrainingPositive)
      sample = negativeLabels.sample(false, propToSample)
    } else {
      val numToSample = (numTrainingPositive / 0.3 * 0.7).toInt
      sample = sc.makeRDD(negativeLabels.takeSample(false, numToSample))
    }

    // Union the negative with the subsampled positive labels
    val adjustedLabels = sc.union(positiveLabels, sample)

    val adjustedPatients = patients.keyBy(_.patientID).join(adjustedLabels)
      .map{ case((pid, (p, label))) => p }

    adjustedPatients
  }

  def runBaseLineModel(sc: SparkContext, trainPatients: RDD[Patient], testPatients: RDD[Patient],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2], firstNoteDates: RDD[FirstNoteInfo],
      labels: RDD[LabelTuple],
      useCv: Boolean=true, numFolds: Int=5,
      interceptCond: Boolean=true, numIterations: Int=300, regParam: Double=0.1) = {
    println("B---------------- Begin baseline model ------------------------")
    var bestInterceptCond = interceptCond
    var bestNumIterations = numIterations
    var bestRegParam = regParam

    /************************* Create model ************************/
    // Split into folds
    if (useCv) {
      val splitProportions = new Array[Double](numFolds)
      for (i <- 0 to (numFolds - 1)) splitProportions(i) = 1.0 / numFolds
      val splits = trainPatients.randomSplit(splitProportions)

      val arrayCvTrainPoints = new Array[RDD[LabeledPoint]](numFolds)
      val arrayCvTestPoints = new Array[RDD[LabeledPoint]](numFolds)
      for (i <- 0 to (numFolds - 1)) {
        val cvTrainTuples = constructBaselineFeatureTuples(
          sc, trainPatients.subtract(splits(i)), icuStays, saps2s)
        arrayCvTrainPoints(i) = constructForSVMSparse(cvTrainTuples, labels)

        val cvTestTuples = constructBaselineFeatureTuples(sc, splits(i), icuStays, saps2s)
        arrayCvTestPoints(i) = constructForSVMSparse(cvTestTuples, labels)
        //arrayCvTestPoints(i).cache
      }

      println("B------- Run cross validation for training model --------")
      /************ Cross validate for the best hyper parameters ***********/
      val interceptConds: List[Boolean] = List(true) // false is default
      val numIterationsParams: List[Int] = List(200, 300, 400) // 100 is default
      val regParams: List[Double] = List(0.1, 1, 10, 50, 100) // 0.0 is default

      var maxAUC = 0.0
      bestInterceptCond = true
      bestNumIterations = 200
      bestRegParam = 0.1

      var sumAUC = 0.0
      for (ic <- interceptConds) {
        for (ni <- numIterationsParams) {
          for (rp <- regParams) {
            val svm = new SVMWithSGD()
            svm.optimizer
              .setNumIterations(ni)
              .setRegParam(rp)
            svm.setIntercept(ic)

            sumAUC = 0.0
            for (i <- 0 to (numFolds - 1)) {
              arrayCvTrainPoints(i).cache
              val svmModel = svm.run(arrayCvTrainPoints(i))
              svmModel.clearThreshold

              val preds = arrayCvTestPoints(i)
                .map(point => (svmModel.predict(point.features), point.label))
              val metrics = new BinaryClassificationMetrics(preds)
              sumAUC += metrics.areaUnderROC
            }
            val auc = sumAUC / numFolds // get average AUC
            println(s"${ic},${ni},${rp},${auc}")
            if (auc > maxAUC) {
              maxAUC = auc
              bestInterceptCond = ic
              bestNumIterations = ni
              bestRegParam = rp
            }
          }
        }
      }
    }

    println(s"Chosen interceptCond: ${bestInterceptCond}")
    println(s"Chosen numIterations: ${bestNumIterations}")
    println(s"Chosen regParam: ${bestRegParam}")

    // Use the best parameters to construct a model for the whole training set
    val trainTuples = constructBaselineFeatureTuples(sc, trainPatients, icuStays, saps2s)
    val trainingPoints = constructForSVMSparse(trainTuples, labels)
    trainingPoints.cache

    val svm = new SVMWithSGD()
    svm.optimizer
      .setNumIterations(bestNumIterations)
      .setRegParam(bestRegParam)
    svm.setIntercept(bestInterceptCond)

    val svmModel = svm.run(trainingPoints)
    //svmModel.clearThreshold // Clears threshold so predict() outputs raw prediction scores.

    println("B----------- Test with time varying testing set --------------")
    // Evaluate the trained model with the training set
    val trainPreds = trainingPoints
      .map(point => (svmModel.predict(point.features), point.label))
    var (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(trainPreds)
    println(s"Number of training instances: ${total}")
    println(s"Number of positive in train: ${numPositives}")
    println(s"Final training AUC: ${auc}")
    println("${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
    println(s"${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")

    println("${hr},${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
    // Test the model across time with the test data
    var go = true
    var hr = 0
    val maxH = 240
    while (hr <= maxH && go) {
      val testTuples = constructBaselineFeatureTuples(sc, testPatients, icuStays,
        saps2s, firstNoteDates, hr)
      val testingPoints = constructForSVMSparse(testTuples, labels)
      // Make predictions using the SVM Model
      val testPreds = testingPoints
        .map(point => (svmModel.predict(point.features), point.label))

      var (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(testPreds)
      println(s"${hr},${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
      hr += 12
    }
    println("B---------------- Baseline model test completed -------------------")
    //println(s"${hours},${numTraining},${numTrainingPositive},${numTesting},${numTestingPositive},${trainAUC},${testAUC}")
  }

  def getBinaryMetrics(predLabels: RDD[(Double, Double)]): (Int, Int, Double, Double, Double, Double) = {
    // (tp, fp, tn, TF)
    val (tp, fp, tn, fn) = predLabels.aggregate((0, 0, 0, 0))(
      (u, pair) => {
        val correct = pair._1 == pair._2
        val positive = pair._2 > 0
        if (correct) {
          if (positive) {
            (u._1+1, u._2, u._3, u._4)
          } else {
            (u._1, u._2, u._3+1, u._4)
          }
        } else {
          if (positive) {
            (u._1, u._2, u._3, u._4+1)
          } else {
            (u._1, u._2+1, u._3, u._4)
          }
        }
      },
      (u1, u2) => (u1._1+u2._1, u1._2+u2._2, u1._3+u2._3, u1._4+u2._4)
    )

    println(s"${tp},${fp},${tn},${fn}")

    val binMetrics = new BinaryClassificationMetrics(predLabels)
    val auc = binMetrics.areaUnderROC

    val total = tp + fp + tn + fn
    val numPositives = tp + fn
    val numNegatives = fp + tn

    val accuracy = (tp + tn).toDouble / total.toDouble
    val sensitivity = tp.toDouble / numPositives // recall
    val specificity = tn.toDouble / numNegatives
    //val precision = tp / (tp + fp)
    (total, numPositives, auc, accuracy, sensitivity, specificity)
  }

  def runRetrospectiveTopicModel(sc: SparkContext,
      trainPatients: RDD[Patient], testPatients: RDD[Patient],
      notes: RDD[Note], stopWords: Set[String],
      firstNoteDates: RDD[FirstNoteInfo], labels: RDD[LabelTuple],
      useCv: Boolean=true, numFolds: Int=5,
      interceptCond: Boolean=true, numIterations: Int=300, regParam: Double=0.1) = {
    println("RTM--------------- Begin retrospective topic model ----------------")

    val noteFeatures = retrospectiveTopicModel(notes, stopWords)
    println(s"note features count: ${noteFeatures.count}")

    var bestInterceptCond = interceptCond
    var bestNumIterations = numIterations
    var bestRegParam = regParam

    if (useCv) {
      /************************* Create model ************************/
      // Split into folds
      val splitProportions = new Array[Double](numFolds)
      for (i <- 0 to (numFolds - 1)) splitProportions(i) = 1.0 / numFolds
      val splits = trainPatients.randomSplit(splitProportions)

      val arrayCvTrainPoints = new Array[RDD[LabeledPoint]](numFolds)
      val arrayCvTestPoints = new Array[RDD[LabeledPoint]](numFolds)
      for (i <- 0 to (numFolds - 1)) {
        val cvTrainTuples = trainPatients.subtract(splits(i))
          .keyBy(_.patientID)
          .join(noteFeatures)
          .map{ case(pid, (p, arr)) => (pid, arr) }
        arrayCvTrainPoints(i) = constructForSVM(cvTrainTuples, labels)

        val cvTestTuples = splits(i)
          .keyBy(_.patientID)
          .join(noteFeatures)
          .map{ case(pid, (p, arr)) => (pid, arr) }
        arrayCvTestPoints(i) = constructForSVM(cvTestTuples, labels)
      }

      println("RTM------- Run cross validation for training model --------")
      /************ Cross validate for the best hyper parameters ***********/
      val interceptConds: List[Boolean] = List(false, true) // false is default
      val numIterationsParams: List[Int] = List(200, 300, 400) // 100 is default
      val regParams: List[Double] = List(0.1, 1, 10, 100) // 0.0 is default

      var maxAUC = 0.0
      bestInterceptCond = false
      bestNumIterations = 200
      bestRegParam = 0.1

      var sumAUC = 0.0
      for (interceptCond <- interceptConds) {
        for (numIterations <- numIterationsParams) {
          for (regParam <- regParams) {
            val svm = new SVMWithSGD()
            svm.optimizer
              .setNumIterations(numIterations)
              .setRegParam(regParam)
            svm.setIntercept(interceptCond)

            sumAUC = 0.0
            for (i <- 0 to (numFolds - 1)) {
              arrayCvTrainPoints(i).cache
              val svmModel = svm.run(arrayCvTrainPoints(i))
              svmModel.clearThreshold

              val preds = arrayCvTestPoints(i)
                .map(point => (svmModel.predict(point.features), point.label))
              val metrics = new BinaryClassificationMetrics(preds)
              sumAUC += metrics.areaUnderROC
            }
            val auc = sumAUC / numFolds // get average AUC
            println(s"${interceptCond},${numIterations},${regParam},${auc}")
            if (auc > maxAUC) {
              maxAUC = auc
              bestInterceptCond = interceptCond
              bestNumIterations = numIterations
              bestRegParam = regParam
            }
          }
        }
      }
    }
    println(s"Best interceptCond: ${bestInterceptCond}")
    println(s"Best numIterations: ${bestNumIterations}")
    println(s"Best regParam: ${bestRegParam}")

    // Use the best parameters to construct a model for the whole training set
    val trainTuples = trainPatients
      .keyBy(_.patientID)
      .join(noteFeatures)
      .map{ case(pid, (p, arr)) => (pid, arr) }
    val trainingPoints = constructForSVM(trainTuples, labels)
    trainingPoints.cache

    // Count number of training instances and positive ones among them
    val (numTraining, numTrainingPositive) = trainingPoints
      .aggregate((0, 0))(
        (u, point) => (u._1+1, u._2+point.label.toInt),
        (u1, u2) => (u1._1+u2._1, u1._2+u2._2)
      )
    println(s"Number of training instances: ${numTraining}")
    println(s"Number of positive in train: ${numTrainingPositive}")

    val svm = new SVMWithSGD()
    svm.optimizer
      .setNumIterations(bestNumIterations)
      .setRegParam(bestRegParam)
    svm.setIntercept(bestInterceptCond)

    val svmModel = svm.run(trainingPoints)
    svmModel.clearThreshold // Clears threshold so predict() outputs raw prediction scores.

    // Evaluate the trained model with the training set
    val trainPreds = trainingPoints
      .map(point => (svmModel.predict(point.features), point.label))
    val trainMetrics = new BinaryClassificationMetrics(trainPreds)
    val trainAUC = trainMetrics.areaUnderROC
    println(s"Final training AUC: ${trainAUC}")

    val testTuples = trainPatients.keyBy(_.patientID).join(noteFeatures)
      .map{ case(pid, (p, arr)) => (pid, arr) }
    val testingPoints = constructForSVM(testTuples, labels)
    val testPreds = testingPoints
      .map(point => (svmModel.predict(point.features), point.label))

    val (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(testPreds)
    println(s"Retrospective Topic Model test AUC: ${auc}")
    println("${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
    println(s"${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
    println("RTM---------------- Retrospective Topic Model completed -------------------")
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
