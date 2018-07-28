import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.{SparkConf, SparkContext}

object ETLPipeline {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val spark = SparkSession.builder.appName("etl-pipeline").getOrCreate()

    if (args.length != 2) {
      println("Two arguments needed: path to sqlite db and output folder path")
    }

    Logger.getLogger("etl-pipeline").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    // pre-requisite: the tables `questions`, `answers`, `postlinks` and `tags` must exist in a sqlite database
    val questions = spark.read.format("jdbc")
      .option("url", "jdbc:sqlite:" + args(0))
      .option("dbtable", "questions")
      .option("customSchema", "creationDate STRING") // spark has issues with parsing timestamp from sqlite table
      .load()

    val answers = spark.read.format("jdbc")
      .option("url", "jdbc:sqlite:" + args(0))
      .option("dbtable", "answers")
      .option("customSchema", "creationDate STRING")
      .load()

    val postlinks = spark.read.format("jdbc")
      .option("url", "jdbc:sqlite:" + args(0))
      .option("dbtable", "postlinks")
      .load()

    val tags = spark.read.format("jdbc")
      .option("url", "jdbc:sqlite:" + args(0))
      .option("dbtable", "tags")
      .load()

    // WARNING: assuming the date column is CreationDate, same as that in original data
    val ts = to_timestamp(col("CreationDate"), "yyyy-MM-dd HH:mm:ss")

    val que = questions.withColumn("date", ts)
    val ans = answers.withColumn("date", ts)

    // long live spark sql
    que.createOrReplaceTempView("questions")
    ans.createOrReplaceTempView("answers")
    tags.createOrReplaceTempView("tags")
    postlinks.createOrReplaceTempView("postlinks")

    // first, get the accepted answer, usually the answer with the highest score
    // best answer for each question
    val answersUnique = spark.sql(
      """
        |select parentid, id from
        |(select *, row_number() over (partition by parentid order by score desc) as rn from answers) tmp
        |where rn = 1
      """.stripMargin)

    answers.createOrReplaceTempView("answers")

    // for now, let's just take 3 tags
    // modify it to choose more
    val origWithTags = spark.sql(
      """
        |select q.id, a.id as answerId, q.date, q.body as text, t.tag
        |from questions q join answers a on q.id = a.parentId
        |join tags t on t.id = q.id
        |where t.tag in ('python', 'java', 'javascript')
      """.stripMargin)

    val dupWithTags = spark.sql(
      """
        |select pl.postId as id, a.id as answerId, q.date, q.body as text, t.tag
        |from postlinks pl join answers a on pl.relatedpostid = a.parentid
        |join tags t on t.id = pl.relatedpostid
        |join questions q on q.id = t.id
        |where t.tag in ('python', 'java', 'javascript') and pl.linktypeid = 3
      """.stripMargin)

    // save to disk
    origWithTags
      .write
      .partitionBy("tag")
      .format("parquet")
      .save(args(1) + "origByTags.parquet")

    dupWithTags
      .write
      .partitionBy("tag")
      .format("parquet")
      .save(args(1) + "dupByTags.parquet")
  }
}
