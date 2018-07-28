import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TextClassifier {

  def trainClassifier(index: Integer, train: DataFrame, test: DataFrame) = {
    val labelColName = "ovrLabel" + index
    val trainingDataset = train.withColumn(labelColName,
      when(col("label") === index.toDouble, 1.0).otherwise(0.0))

    // weight col
    val negatives = trainingDataset.filter(trainingDataset(labelColName) === 0).count
    val size = trainingDataset.count
    val ratio = (size - negatives).toDouble / size

    val calculateWt = udf { d: Double =>
      if (d == 0.0) {
        1 * ratio
      } else {
        1 * (1 - ratio)
      }
    }

    val weightedTrainingDataset = trainingDataset.withColumn("classWeight",
      calculateWt(trainingDataset(labelColName)))

    val classifier = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol(labelColName)
      .setProbabilityCol("prob")

    val out = classifier.fit(weightedTrainingDataset).transform(weightedTrainingDataset)

    val vectorToColumn = udf { (x: DenseVector, index: Int) => x(index) }

    out.withColumn("prob" + index, vectorToColumn(col("prob"), lit(1)))
      .select("id", "answerId", "prob" + index)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val spark = SparkSession.builder.appName("Text Classifier").getOrCreate()

    if (args.length != 3) {
      println("Three arguments needed")
    }

    Logger.getLogger("qa-matching").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    // Read data
    var questions = spark.read.option("header", "false")
      .format("csv")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(args(0))
      .toDF("id", "answerId", "text", "date")

    var duplicates = spark.read.option("header", "false")
      .format("csv")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(args(1))
      .toDF("id", "answerId", "text", "date")

    var answers = spark.read.option("header", "false")
      .format("csv")
      .option("delimiter", "\t")
      .load(args(2))
      .toDF("id", "text")

    // Temp views for programmatically querying using SQL
    questions.createOrReplaceTempView("questions")
    duplicates.createOrReplaceTempView("duplicates")
    answers.createOrReplaceTempView("answers")

    // appending rank
    val duplicatesRanked = spark.sql("select d.id, d.answerId, d.text, d.date, percent_rank() over (partition by d.answerId order by d.date) as rank from duplicates d")
    val data = duplicatesRanked.withColumn("cleanText", regexp_replace(col("text"), lit("<pre><code>.*?</code></pre>|<[^>]+>|<a[^>]+>(.*)</a>|"), lit("")))
    data.createOrReplaceTempView("data")

    var train = spark.sql("select id, answerId, text, date from data where rank < 0.75")
    var test = spark.sql("select id, answerId, text, date from data where rank >= 0.75")

    train.createOrReplaceTempView("train")
    test.createOrReplaceTempView("test")

    // create training and testing datasets
    train = spark.sql("select * from train union select * from questions")
    train.createOrReplaceTempView("train")

    // select only those answers that have at least some number of duplicate questions in training set
    var usefulAnswers = spark.sql("select answerId, count(id) as n_samples from train group by answerId having n_samples > 150")
    usefulAnswers.createOrReplaceTempView("usefulAnswers")

    train = spark.sql("select * from train where answerId in (select answerId from usefulAnswers)")
    test = spark.sql("select * from test where answerId in (select answerId from usefulAnswers)")

    // now, set up pipeline
    val tokenizer = new Tokenizer()
      .setInputCol("cleanText")
      .setOutputCol("tokenizerOut")
    val stopWordsFilter = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("stopWordsFilterOut")
    val hashingTf = new HashingTF()
      .setInputCol(stopWordsFilter.getOutputCol)
      .setOutputCol("hashingTfOut")
      .setNumFeatures(50)
    val idf = new IDF()
      .setInputCol(hashingTf.getOutputCol)
      .setOutputCol("features")
    val labelIndexer = new StringIndexer()
      .setInputCol("answerId")
      .setOutputCol("label")

    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsFilter, hashingTf, idf, labelIndexer))

    // train and test features
    var trainFeatures = pipeline.fit(train).transform(train)
    var testFeatures = pipeline.fit(test).transform(test)

    val numClasses = trainFeatures.select("label").distinct().count.toInt
    val output = Range(0, numClasses)
      .map(x => trainClassifier(x, trainFeatures, testFeatures))
      .reduce((x, y) => x.join(y, Seq("id", "answerId")))

    // also add original label column
    val liOut = labelIndexer.fit(output).transform(output)

    // vector assembler to assemble all probabilities in one vector
    val assembler = new VectorAssembler()
      .setInputCols(Range(0, numClasses).map(x => "prob" + x).toArray)
      .setOutputCol("allProbs")
    val asOut = assembler.transform(liOut)
    val predictions = asOut.select("id", "answerId", "label", "allProbs")

    val getRank = udf { (prob: DenseVector, label: Double) =>
      val arrIndex = label.toInt
      val probArr = prob.toArray
      // now get rank
      val rankArr = probArr.zipWithIndex
        .map(_.swap)
        .sortBy(-_._2)
        .map(_._1)
        .zipWithIndex
        .sortBy(_._1)
        .map(x => x._2 + 1)
      rankArr(arrIndex)
    }

    val finalOut = predictions.withColumn("rank",
      getRank(col("allProbs"), col("label")))
    finalOut.select(avg(col("rank"))).show()
  }

}
