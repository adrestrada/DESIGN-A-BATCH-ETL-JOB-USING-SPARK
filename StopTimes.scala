package ca.mcit.bigdata.project5.model

case class StopTimes(trip_id : String,
                     arrival_time : String,
                     departure_time : String,
                     stop_id : String,
                     stop_sequence : Int)

object StopTimes {
  def apply(csv: String): StopTimes = {
    val n = csv.split(",", -1)
    StopTimes(n(0), n(1), n(2), n(3), n(4).toInt)
  }
}