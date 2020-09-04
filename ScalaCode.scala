import java.io.{File, PrintWriter}

import scala.io.Source

object ScalaCode {    //options is a container that can give us two values: some or none
  def main(args:Array[String]): Unit ={
    println("This is %d percent %s".format(100,"Original"))   //formatted

    val read_usa=Source.fromFile("C:\\Users\\LENOVO\\OneDrive\\Documents\\learners'Docs\\usa_barley.csv")
    for (line <- read_usa.getLines) {
      val cols = line.split(",").map(_.trim)
      println(s"${cols(0)}|${cols(1)}")
    }
    read_usa.close()

    val read_world=Source.fromFile("C:\\Users\\LENOVO\\OneDrive\\Documents\\learners'Docs\\world_barley.csv")
    for (line <- read_world.getLines) {
      val cols = line.split(",").map(_.trim)
      println(s"${cols(0)}|${cols(1)}")
    }
    read_world.close()
  }
}
