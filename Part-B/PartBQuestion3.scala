
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.ProcessingTime


object CountTweets {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: CountTweets <streaming path> <userlist>")
      System.exit(1)
    }

    val streaming_path = args(0)

    val spark = SparkSession
      .builder
      .appName("Count Tweets")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // val lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    // val users = spark.sparkContext.textFile(sys.argv[1])
    //($"name" isin ("A","B"))

    import scala.io.Source
    val listOfUsers = Source.fromFile(args(1)).getLines.toList

    val userSchema = new StructType().add("user1", "integer")
        .add("user2", "integer")
        .add("timestamp", "timestamp")
        .add("interaction", "string")

    val tweets = spark
          .readStream
          .option("sep", ",")
          .schema(userSchema)
          .csv(streaming_path)

    // Generate running word count
    val windowedCounts = tweets.select("user1").where($"user1".isin(listOfUsers:_*))
                          .groupBy("user1").count()

    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: Row) = println(value(0)+"\t\t: "+value(1))
      override def close(errorOrNull: Throwable) = { }
    }

    // Start running the query that prints the running counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .foreach(writer)
      .option("truncate", "false")
      .trigger(ProcessingTime("5 seconds"))
      .start()

    println("==========ready for streaming============")
    query.awaitTermination()
  }
}
