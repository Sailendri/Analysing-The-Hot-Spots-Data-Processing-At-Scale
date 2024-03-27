package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()
  pickupInfo.createOrReplaceTempView("details")
  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  
  val given_values_df = spark.sql("select x,y,z from details where x >= " + minX + " and x <= " + maxX + " and y >= " + minY + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ + " order by z,y,x").persist();
  given_values_df.createOrReplaceTempView("xyz_details")
  
  val given_values_count_df = spark.sql("select x,y,z,count(*) as num_of_pts from xyz_details group by z,y,x order by z,y,x").persist();
  given_values_count_df.createOrReplaceTempView("xyz_count_details")//hotcellcountdetails

  spark.udf.register("square_func",(value:Int)=>(HotcellUtils.square_func(value)))

  val given_values_sum_df = spark.sql("select sum(num_of_pts) as sum_of_pts, sum(square_func(num_of_pts)) as sum_of_sq_pts from xyz_count_details");
  given_values_sum_df.createOrReplaceTempView("xyz_sum_details")


  // Xj = number of pickups in a particular location = sum of points
  val sum_of_pts = given_values_sum_df.first().getLong(0);
  // X-bar
  val mean_of_count = (sum_of_pts.toDouble / numCells.toDouble).toDouble;
  // S
  val sum_of_sq_pts = given_values_sum_df.first().getDouble(1);
  val st_dev = math.sqrt((sum_of_sq_pts.toDouble / numCells.toDouble) - (mean_of_count.toDouble * mean_of_count.toDouble)).toDouble
  
  // Neighbors count
  spark.udf.register("NeighbourCount", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, x_val: Int, y_val: Int, z_val: Int)
  => ((HotcellUtils.NeighbourCount(minX, minY, minZ, maxX, maxY, maxZ, x_val, y_val, z_val))))
  val Neighbors_count = spark.sql("select NeighbourCount("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x,a1.y,a1.z) as adj_cell_count,count(*) as countall, a1.x as x,a1.y as y,a1.z as z, sum(a2.num_of_pts) as hotness from xyz_count_details as a1, xyz_count_details as a2 where (a2.x = a1.x+1 or a2.x = a1.x or a2.x = a1.x-1) and (a2.y = a1.y+1 or a2.y = a1.y or a2.y =a1.y-1) and (a2.z = a1.z+1 or a2.z = a1.z or a2.z =a1.z-1) group by a1.z,a1.y,a1.x order by a1.z,a1.y,a1.x").persist();
  Neighbors_count.createOrReplaceTempView("neighbor_details");
  
  //Calculate getisOrdStatistic value
  spark.udf.register("g_score_calculation", (x: Int, y: Int, z: Int, neighbour_cell_count: Int, sum_of_pickups_total: Int, mean: Double, std_dev: Double, no_of_cells: Double) => ((HotcellUtils.g_score_calculation(x,y,z,neighbour_cell_count,sum_of_pickups_total,mean_of_count,st_dev,numCells))))
  val zscore = spark.sql("select x,y,z,g_score_calculation(x,y,z,adj_cell_count,hotness,"+mean_of_count+","+st_dev+","+numCells+") as getisOrdStatistic from neighbor_details order by getisOrdStatistic desc");
  zscore.createOrReplaceTempView("zscore_df")

  val answer_df2 = spark.sql("select x,y,z from zscore_df") 
  answer_df2.createOrReplaceTempView("Result")

  return answer_df2
}
}

