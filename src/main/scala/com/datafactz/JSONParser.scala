package com.datafactz

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.io.Source
import org.joda.time._

/**
 * Created by snudurupati on 12/24/15.
 */
object JSONParser {

  def main(args : Array[String]) {
    val JSONString = Source.fromFile("data/rest.json").mkString
    val json = parse(JSONString)

    val svcDts = for {
      JObject(patient) <- json
      JField("svcDt", JInt(svcDt)) <- patient
    } yield svcDt

    println(svcDts)
    val dt = new DateTime(1235019600000L).toString("yyyy-MM-dd HH:MM:SS")
    println(dt)
  }

}
