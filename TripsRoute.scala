package ca.mcit.bigdata.project5.model

case class TripsRoute(trips: Trips,
                       routes: Routes)

object TripsRoute {

  def toCsv(tripsRoute: TripsRoute): String = {
    s"${tripsRoute.trips}," +
      s"${tripsRoute.routes}"
  }
}

