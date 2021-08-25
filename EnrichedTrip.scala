package ca.mcit.bigdata.project5.model

case class EnrichedTrip(tripsRoute: TripsRoute,
                        calendarDates: CalendarDates)

object EnrichedTrip {
  def toCsv(enrichedTrip: EnrichedTrip): String = {
    s"${enrichedTrip.tripsRoute.trips.tripId}," +
      s"${enrichedTrip.calendarDates.serviceId}, " +
      s"${enrichedTrip.tripsRoute.routes.routeId}, " +
      s"${enrichedTrip.tripsRoute.trips.tripHeadsign}," +
      s"${enrichedTrip.tripsRoute.trips.wheelchairAccessible}," +
      s"${enrichedTrip.calendarDates.date}," +
      s"${enrichedTrip.calendarDates.exceptionType}," +
      s"${enrichedTrip.tripsRoute.routes.routeLongName}," +
      s"${enrichedTrip.tripsRoute.routes.routeColor}"
  }
}



