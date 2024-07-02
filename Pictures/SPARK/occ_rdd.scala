import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
def main(args: Array[String]): Unit = {
val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
val sc = new SparkContext(conf)
// Path to the text file
val pathToFile = "txt.txt"
// Read the text file and create an RDD of lines
val linesRdd = sc.textFile(pathToFile)
// Split each line into words and flatten the result
val wordsRdd = linesRdd.flatMap(_.split("\\s+"))
// Map each word to a tuple (word, 1) for counting
val wordCountRdd = wordsRdd.map(word => (word, 1))
// Reduce by key to get the count of each word
val wordCounts = wordCountRdd.reduceByKey(_ + _)
// Print the word counts
wordCounts.foreach(println)

sc.stop()
}
}
