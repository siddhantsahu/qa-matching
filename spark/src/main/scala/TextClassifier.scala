import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

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
  }

}
