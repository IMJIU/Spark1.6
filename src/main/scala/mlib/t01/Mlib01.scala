//package com.t01
//
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by admin on 2016/6/21.
//  */
//object Mlib01 {
//
//  val conf = new SparkConf().setAppName("T01").setMaster("local")
//  val sc = new SparkContext(conf)
//  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//  def main(args: Array[String]) {
//    // this is used to implicitly convert an RDD to a DataFrame.
//    import sqlContext.implicits._
//    example1
//
////    example2
////
////
////    example3
////
////    example4
//  }
//
//  def example4: Any = {
//    import org.apache.spark.ml.evaluation.RegressionEvaluator
//    import org.apache.spark.ml.regression.LinearRegression
//    import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
//
//    // Prepare training and test data.
//    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
//    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)
//
//    val lr = new LinearRegression()
//
//    // We use a ParamGridBuilder to construct a grid of parameters to search over.
//    // TrainValidationSplit will try all combinations of values and determine best model using
//    // the evaluator.
//    val paramGrid = new ParamGridBuilder()
//      .addGrid(lr.regParam, Array(0.1, 0.01))
//      .addGrid(lr.fitIntercept)
//      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
//      .build()
//
//    // In this case the estimator is simply the linear regression.
//    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
//    val trainValidationSplit = new TrainValidationSplit()
//      .setEstimator(lr)
//      .setEvaluator(new RegressionEvaluator)
//      .setEstimatorParamMaps(paramGrid)
//      // 80% of the data will be used for training and the remaining 20% for validation.
//      .setTrainRatio(0.8)
//
//    // Run train validation split, and choose the best set of parameters.
//    val model = trainValidationSplit.fit(training)
//
//    // Make predictions on test data. model is the model with combination of parameters
//    // that performed best.
//    model.transform(test)
//      .select("features", "label", "prediction")
//      .show()
//  }
//
//  def example3: Unit = {
//    import org.apache.spark.ml.Pipeline
//    import org.apache.spark.ml.classification.LogisticRegression
//    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
//    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
//    import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
//    import org.apache.spark.mllib.linalg.Vector
//    import org.apache.spark.sql.Row
//
//    // Prepare training data from a list of (id, text, label) tuples.
//    val training = sqlContext.createDataFrame(Seq(
//      (0L, "a b c d e spark", 1.0),
//      (1L, "b d", 0.0),
//      (2L, "spark f g h", 1.0),
//      (3L, "hadoop mapreduce", 0.0),
//      (4L, "b spark who", 1.0),
//      (5L, "g d a y", 0.0),
//      (6L, "spark fly", 1.0),
//      (7L, "was mapreduce", 0.0),
//      (8L, "e spark program", 1.0),
//      (9L, "a e c l", 0.0),
//      (10L, "spark compile", 1.0),
//      (11L, "hadoop software", 0.0)
//    )).toDF("id", "text", "label")
//
//    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
//    val tokenizer = new Tokenizer()
//      .setInputCol("text")
//      .setOutputCol("words")
//    val hashingTF = new HashingTF()
//      .setInputCol(tokenizer.getOutputCol)
//      .setOutputCol("features")
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//    val pipeline = new Pipeline()
//      .setStages(Array(tokenizer, hashingTF, lr))
//
//    // We use a ParamGridBuilder to construct a grid of parameters to search over.
//    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
//    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
//    val paramGrid = new ParamGridBuilder()
//      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
//      .addGrid(lr.regParam, Array(0.1, 0.01))
//      .build()
//
//    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
//    // This will allow us to jointly choose parameters for all Pipeline stages.
//    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
//    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
//    // is areaUnderROC.
//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(new BinaryClassificationEvaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(2) // Use 3+ in practice
//
//    // Run cross-validation, and choose the best set of parameters.
//    val cvModel = cv.fit(training)
//
//    // Prepare test documents, which are unlabeled (id, text) tuples.
//    val test = sqlContext.createDataFrame(Seq(
//      (4L, "spark i j k"),
//      (5L, "l m n"),
//      (6L, "mapreduce spark"),
//      (7L, "apache hadoop")
//    )).toDF("id", "text")
//
//    // Make predictions on test documents. cvModel uses the best model found (lrModel).
//    cvModel.transform(test)
//      .select("id", "text", "probability", "prediction")
//      .collect()
//      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//      }
//  }
//
//  def example2: Unit = {
//    import org.apache.spark.ml.{Pipeline, PipelineModel}
//    import org.apache.spark.ml.classification.LogisticRegression
//    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
//    import org.apache.spark.mllib.linalg.Vector
//    import org.apache.spark.sql.Row
//
//    // Prepare training documents from a list of (id, text, label) tuples.
//    val training = sqlContext.createDataFrame(Seq(
//      (0L, "a b c d e spark", 1.0),
//      (1L, "b d", 0.0),
//      (2L, "spark f g h", 1.0),
//      (3L, "hadoop mapreduce", 0.0)
//    )).toDF("id", "text", "label")
//
//    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
//    val tokenizer = new Tokenizer()
//      .setInputCol("text")
//      .setOutputCol("words")
//    val hashingTF = new HashingTF()
//      .setNumFeatures(1000)
//      .setInputCol(tokenizer.getOutputCol)
//      .setOutputCol("features")
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.01)
//    val pipeline = new Pipeline()
//      .setStages(Array(tokenizer, hashingTF, lr))
//
//    // Fit the pipeline to training documents.
//    val model = pipeline.fit(training)
//
//    // now we can optionally save the fitted pipeline to disk
//    model.save("/tmp/spark-logistic-regression-model")
//
//    // we can also save this unfit pipeline to disk
//    pipeline.save("/tmp/unfit-lr-model")
//
//    // and load it back in during production
//    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")
//
//    // Prepare test documents, which are unlabeled (id, text) tuples.
//    val test = sqlContext.createDataFrame(Seq(
//      (4L, "spark i j k"),
//      (5L, "l m n"),
//      (6L, "mapreduce spark"),
//      (7L, "apache hadoop")
//    )).toDF("id", "text")
//
//    // Make predictions on test documents.
//    model.transform(test)
//      .select("id", "text", "probability", "prediction")
//      .collect()
//      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//      }
//  }
//
//  def example1: Unit = {
//    import org.apache.spark.ml.classification.LogisticRegression
//    import org.apache.spark.ml.param.ParamMap
//    import org.apache.spark.mllib.linalg.{Vector, Vectors}
//    import org.apache.spark.sql.Row
//
//    // Prepare training data from a list of (label, features) tuples.
//    val training = sqlContext.createDataFrame(Seq(
//      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
//      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
//      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
//      (1.0, Vectors.dense(0.0, 1.2, -0.5))
//    )).toDF("label", "features")
//
//    // Create a LogisticRegression instance.  This instance is an Estimator.
//    val lr = new LogisticRegression()
//    // Print out the parameters, documentation, and any default values.
//    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
//
//    // We may set parameters using setter methods.
//    lr.setMaxIter(10).setRegParam(0.01)
//
//    // Learn a LogisticRegression model.  This uses the parameters stored in lr.
//    val model1 = lr.fit(training)
//    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
//    // we can view the parameters it used during fit().
//    // This prints the parameter (name: value) pairs, where names are unique IDs for this
//    // LogisticRegression instance.
//    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)
//
//    // We may alternatively specify parameters using a ParamMap,
//    // which supports several methods for specifying parameters.
//    val paramMap = ParamMap(lr.maxIter -> 20)
//      .put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
//      .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.
//
//    // One can also combine ParamMaps.
//    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name
//    val paramMapCombined = paramMap ++ paramMap2
//
//    // Now learn a new model using the paramMapCombined parameters.
//    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
//    val model2 = lr.fit(training, paramMapCombined)
//    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)
//
//    // Prepare test data.
//    val test = sqlContext.createDataFrame(Seq(
//      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
//      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
//      (1.0, Vectors.dense(0.0, 2.2, -1.5))
//    )).toDF("label", "features")
//
//    // Make predictions on test data using the Transformer.transform() method.
//    // LogisticRegression.transform will only use the 'features' column.
//    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
//    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
//    model2.transform(test)
//      .select("features", "label", "myProbability", "prediction")
//      .collect()
//      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
//        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
//      }
//  }
//}
