package ca.mcit.bigdata.project5.model

case class CalendarDates(serviceId: String,
                          date: String,
                          exceptionType: Int)

object CalendarDates {
  def apply(csv: String): CalendarDates = {
    val values3 = csv.split(",", -1)
    CalendarDates(values3(0), values3(1), values3(2).toInt)
  }

  def toCsv(calendarDates: CalendarDates): String = {
    s"${calendarDates.serviceId},${calendarDates.date},${calendarDates.exceptionType}"
  }
}
