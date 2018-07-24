import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer,HashingTF, IDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}

object TextClassifier {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val spark = SparkSession.builder.appName("Text Classifier").getOrCreate()

    if (args.length != 3) {
      println("Three arguments needed")
    }

    Logger.getLogger("sc-airlines").setLevel(Level.OFF)
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
    val data = spark.sql("select d.id, d.answerId, d.text, d.date, percent_rank() " +
      "over (partition by d.answerId order by d.date) as rank from duplicates d")
    data.createOrReplaceTempView("data")

    var train = spark.sql("select id, answerId, text, date from data where rank < 0.75")
    var test = spark.sql("select id, answerId, text, date from data where rank >= 0.75")

    train.createOrReplaceTempView("train")
    test.createOrReplaceTempView("test")

    // creating train and test datasets
    train = spark.sql("select * from train union select * from questions")
    train.createOrReplaceTempView("train")

    // select only those answers that have at least 10 duplicate questions in training set
    var ans10Plus = spark.sql("select answerId, count(id) as n_samples from train group by answerId having n_samples > 12")
    ans10Plus.createOrReplaceTempView("ans10Plus")

    train = spark.sql("select * from train where answerId in (select answerId from ans10Plus)")
    test = spark.sql("select * from test where answerId in (select answerId from ans10Plus)")

    train.show()

    //====================================================================================================================
    //Regex to remove html tags and links
    val train_1 = train.withColumn("Clear_Text", regexp_replace($"text" , lit("<pre><code>.*?</code></pre>|<[^>]+>|<a[^>]+>(.*)</a>|"), lit("")))

    val tokenizer = new Tokenizer().setInputCol("Clear_Text").setOutputCol("Clear_Words").transform(train_1)

    tokenizer.show(truncate=true)

    val remover = new StopWordsRemover().
      setInputCol("Clear_Words").
      setOutputCol("Clear_Word_SWR").transform(tokenizer)

    val wordsData = remover.show(truncate=true)

    val hashingTF = new HashingTF().
      setInputCol("Clear_Words").setOutputCol("rawFeatures").setNumFeatures(500)

    val featurizedData = hashingTF.transform(remover)


    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("idfFeat")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show()

    val featureCols = Array("idfFeat")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(rescaledData)
    df2.show()

    val labelIndexer = new StringIndexer().setInputCol("answerId").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)

    df3.show()

    val classifier = new LogisticRegression().setMaxIter(2)

    val ovr = new OneVsRest().setClassifier(classifier)

    val ovrModel = ovr.fit(df3)

    val predictions = ovrModel.transform(df3)

    predictions.select ("features", "label", "prediction").show()
  }

}
