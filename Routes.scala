package ca.mcit.bigdata.project5.model

case class Routes(routeId: Int,
                   routeLongName: String,
                   routeColor: String)

object Routes {
  def apply(csv: String): Routes = {
    val values2 = csv.split(",", -1)
    Routes(values2(0).toInt, values2(3), values2(6))
  }

  def toCsv(routes: Routes): String = {
    s"${routes.routeId},${routes.routeLongName},${routes.routeColor}"
  }
}
