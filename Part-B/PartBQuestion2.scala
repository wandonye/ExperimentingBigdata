import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, ArrayType, StringType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.ProcessingTime

object CountTweets {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: CountTweets <streaming path> <output path>")
      System.exit(1)
    }

    val streaming_path = args(0)

    val spark = SparkSession
      .builder
      .appName("CountTweets")
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

    val windowedCounts = tweets.select("user2").where("interaction='MT'")

    val query = windowedCounts.writeStream
      .format("parquet")
      .option("path", args(1))
      .option("checkpointLocation", "checkpoint")
      .trigger(ProcessingTime("10 seconds"))
      .start()

    println("==========ready for streaming============")
    query.awaitTermination()
  }
}
