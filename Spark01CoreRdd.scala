package ca.mcit.bigdata.project5

import ca.mcit.bigdata.project5.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01CoreRdd extends App with Base {
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~spark configuration
  val sparkConf = new SparkConf()
    .setAppName("Spark Assignament#2")
    .setMaster("local[*]")
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~spark context
  val sc = new SparkContext(sparkConf)
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Trips
  val trips: RDD[String] = sc
    .textFile("/user/bdsf2001/adriest/project5/trips/trips.txt")
  val tripRdd: RDD[Trips] = trips
    .filter(!_.contains("route_id"))
    .map(Trips(_))
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~CalendarDates
  val calendarDates = sc
    .textFile("/user/bdsf2001/adriest/project5/calendar_dates/calendar_dates.txt")
  val calendarDateRdd: RDD[CalendarDates] = calendarDates
    .filter(!_.contains("service_id"))
    .map(CalendarDates(_))
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Routes
  val routes = sc
    .textFile("/user/bdsf2001/adriest/project5/routes.txt")
  val routeRdd: RDD[Routes] = routes
    .filter(!_.contains("route_id"))
    .map(Routes(_))
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~prepare RDD to join
  val tripsRddKey: RDD[(String, Trips)] = tripRdd
    .keyBy(_.serviceId)
  val calendarRddKey: RDD[(String, CalendarDates)] = calendarDateRdd
    .keyBy(_.serviceId)
  val routesRddKey: RDD[(Int, Routes)] = routeRdd
    .keyBy(_.routeId)
  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ join
  val result: RDD[(Int, (String, (Trips, CalendarDates)))] = tripsRddKey
    .join(calendarRddKey)
    .keyBy(x => x._2._1.routeId)

  val enrichedTrip: RDD[String] = result
    .join(routesRddKey)
    .map {
      case (_, ((_, (trips: Trips, calendarDates: CalendarDates)), routes: Routes)) =>
        EnrichedTrip.toCsv(EnrichedTrip(TripsRoute(trips, routes), calendarDates))
    }
  enrichedTrip
    .take(50)
  sc.stop()
}



