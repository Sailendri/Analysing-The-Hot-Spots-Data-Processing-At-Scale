package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
   val given_rectangle = queryRectangle.split(",")
    val rect_x_pt1 = given_rectangle(0).trim().toDouble
    val rect_x_pt2 = given_rectangle(2).trim().toDouble
    val rect_y_pt1 = given_rectangle(1).trim().toDouble
    val rect_y_pt2 = given_rectangle(3).trim().toDouble

    val given_point = pointString.split(",")
    val pt_x = given_point(0).trim().toDouble
    val pt_y = given_point(1).trim().toDouble

    var mini_x: Double = 0
    var max_x: Double = 0
   
    if(rect_x_pt1 > rect_x_pt2)
    {
      max_x = rect_x_pt1
      mini_x = rect_x_pt2
    }
    else
    {
      max_x = rect_x_pt2
      mini_x = rect_x_pt1 
    }
    
    var mini_y: Double = 0
    var max_y: Double = 0
   
    if(rect_y_pt1 > rect_y_pt2)
    {
      max_y = rect_y_pt1
      mini_y = rect_y_pt2
    }
    else
    {
      max_y = rect_y_pt2
      mini_y = rect_y_pt1 
    }

    if (pt_x >= mini_x && pt_x <= max_x && pt_y >= mini_y && pt_y <= max_y)
        {
          return true 
        }
    else
    {
       return false
    }
}
  // YOU NEED TO CHANGE THIS PART

}
