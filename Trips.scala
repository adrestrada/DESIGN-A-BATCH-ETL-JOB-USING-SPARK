package ca.mcit.bigdata.project5.model

case class Trips(routeId: Int,
                 serviceId: String,
                 tripId : String,
                 tripHeadsign: String,
                 wheelchairAccessible: Boolean)

object Trips {
  def apply(csv: String): Trips = {
    val n = csv.split(",", -1)
    Trips(n(0).toInt, n(1), n(2), n(3), n(6) == "1")
  }
  def toCsv(trips: Trips): String = {
    s"${trips.routeId},${trips.serviceId},${trips.tripId},${trips.tripHeadsign},${trips.wheelchairAccessible}"
  }
}