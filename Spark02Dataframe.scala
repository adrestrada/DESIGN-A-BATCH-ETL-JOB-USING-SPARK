package ca.mcit.bigdata.project5

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ca.mcit.bigdata.project5.model.StopTimes
import org.apache.spark.rdd.RDD

object Spark02Dataframe extends App with Base {

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~SPARK SESSION
  val fileDirProject5 = "/user/bdsf2001/adriest/project5/"

  val spark = SparkSession
    .builder()
    .appName("Spark streaming project5")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~read
  val tripsDataFrame = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/bdsf2001/adriest/project5/trips/trips.txt")

  val routesDataFrame = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/bdsf2001/adriest/project5/routes/routes.txt")

  val calendarDatesDataFrame = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/bdsf2001/adriest/project5/calendar_dates/calendar_dates.txt")

  tripsDataFrame.createOrReplaceTempView("trips")
  routesDataFrame.createOrReplaceTempView("routes")
  calendarDatesDataFrame.createOrReplaceTempView("calendarDates")
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Data frame sql
  val enrichedTrip: DataFrame = spark.sql(
    """ SELECT
      |  t.trip_id,
      |  t.service_id,
      |  t.route_id,
      |  t.trip_headsign,
      |  t.wheelchair_accessible,
      |  c.`date`,
      |  c.exception_type,
      |  r.route_long_name,
      |  r.route_color
      |  FROM trips t
      |  JOIN calendarDates c ON (t.service_id = c.service_id)
      |  JOIN routes r ON (t.route_id = r.route_id)""".stripMargin)
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~kafka consuming
  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "adriest1",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val topicName = "bdsf2001_adriest_stop_times"
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~streaming
  val inStream: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topicName), kafkaConfig)
    )
  val stopTimes: DStream[String] = inStream.map(_.value())

  import spark.implicits._

  stopTimes.foreachRDD(microRDD => businessLogic(microRDD))

  def businessLogic(microRDD: RDD[String]): Unit = {
    val stopTimesDataFrame: DataFrame = microRDD
      .map(StopTimes(_))
      .toDF

    val output: DataFrame = stopTimesDataFrame
      .join(enrichedTrip, "trip_id")
      .select("trip_id", "service_id", "route_id", "trip_headsign", "date",
        "exception_type", "route_long_name", "route_color", "arrival_time",
        "departure_time", "stop_id", "stop_sequence", "wheelchair_accessible")

    output
      .write
      .mode("append")
      .format("csv")
      .save("/user/bdsf2001/adriest/project5/enriched_stop_times/")
  }
  ssc.start()
  ssc.awaitTerminationOrTimeout(5)
  ssc.stop(stopSparkContext = true, stopGracefully = true)
  spark.close()
}




