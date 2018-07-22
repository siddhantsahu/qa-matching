import java.io.IOException
import scala.io.Source

object CustomTokenizer {

  def main(args: Array[String]): Unit = {
    val fileName: String = "./duplicates_questions_reformatted.csv"
    var line: String = ""
    val cvsSplitBy: String = ","
    try {
      for(line <- Source.fromFile(fileName).getLines) {
          val text: Array[String] = line.split(cvsSplitBy)
          for (i <- 0 until text.length) {
            text(i) = text(i).replaceAll("<pre><code>.*?</code></pre>","")
            text(i) = text(i).replaceAll("<a[^>]+>(.*)</a>","")
            text(i) = text(i).replaceAll("<[^>]+>","")

            //Removing links
            text(i) = text(i).replaceAll("[a-z]+://","")

            println(text(i))
          }
        }
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }
}