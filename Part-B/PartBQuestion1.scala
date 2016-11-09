
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.ForeachWriter

object CountTweets {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: CountTweets <streaming path>")
      System.exit(1)
    }

    val streaming_path = args(0)

    val spark = SparkSession
      .builder
      .appName("Count Tweets")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

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
    val windowedCounts = tweets.groupBy(
              window($"timestamp", "1 hour", "30 minutes"),
              $"interaction"
            ).count().sort(asc("window"))

    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: Row) = println("| "+value(0)+"\t| "+value(1)+"\t| "+value(2)+"\t|")
      override def close(errorOrNull: Throwable) = { }
    }
    // Start running the query that prints the running counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .foreach(writer)
      .option("truncate", "false")
      .start()

    println("==========ready for streaming============")
    query.awaitTermination()
  }
}
