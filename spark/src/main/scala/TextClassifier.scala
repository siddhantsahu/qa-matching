import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TextClassifier {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val spark = SparkSession.builder.appName("qna-matching").getOrCreate()

    // TODO: integrate a command line parser
    if (args.length != 2) {
      println("Two arguments needed: path to questions.csv and duplicates.csv")
    }

    Logger.getLogger("qna-matching").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    // Read data
    // TODO: If location is a S3 path, check if compressed version of files are supported
    // Refer https://docs.aws.amazon.com/emr/latest/ManagementGuide/HowtoProcessGzippedFiles.html
    var questions = spark.read
      .format("parquet")
      .load(args(0))

    var duplicates = spark.read
      .format("parquet")
      .load(args(1))

    // Create temp views for programmatically querying using SQL
    questions.createOrReplaceTempView("questions")
    duplicates.createOrReplaceTempView("duplicates")

    // appending rank to split dataset into train and test
    val duplicatesRanked = spark.sql(
      """
        |select d.id, d.answerId, d.text, d.date,
        |percent_rank() over (partition by d.answerId order by rand()) as rank
        |from duplicates d
      """.stripMargin)
    duplicatesRanked.createOrReplaceTempView("data")

    var train = spark.sql("select id, answerId, text, date from data where rank < 0.75")
    var test = spark.sql("select id, answerId, text, date from data where rank >= 0.75")

    train.createOrReplaceTempView("train")
    test.createOrReplaceTempView("test")

    // create training and testing datasets
    train = spark.sql("select * from train union select * from questions")
    train.createOrReplaceTempView("train")

    // select only those answers that have at least some number of duplicate questions in training set
    // default should be 12 or 13, increase to 150 for testing (vastly reduces size of training dataset)
    val threshold = 50
    var usefulAnswers = spark.sql(
      s"select answerId, count(id) as n_samples from train group by answerId having n_samples > $threshold")
    usefulAnswers.createOrReplaceTempView("usefulAnswers")

    train = spark.sql("select * from train where answerId in (select answerId from usefulAnswers)")
    test = spark.sql("select * from test where answerId in (select answerId from usefulAnswers)")

    // clean text
    train = train.withColumn("cleanText",
      regexp_replace(col("text"),
        lit("<pre><code>.*?</code></pre>|<[^>]+>|<a[^>]+>(.*)</a>|"), lit("")))

    test = test.withColumn("cleanText",
      regexp_replace(col("text"),
        lit("<pre><code>.*?</code></pre>|<[^>]+>|<a[^>]+>(.*)</a>|"), lit("")))

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

    // Get rank of an array, example [5, 2, 8, 3] => [3, 1, 4, 2]
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

    // prints the mean rank for this classifier
    finalOut.select(avg(col("rank"))).show()
  }

  def trainClassifier(index: Integer, train: DataFrame, test: DataFrame) = {
    val labelColName = "ovrLabel" + index
    val trainingDataset = train.withColumn(labelColName,
      when(col("label") === index.toDouble, 1.0).otherwise(0.0))

    // weight column to handle imbalanced datasets
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
}
