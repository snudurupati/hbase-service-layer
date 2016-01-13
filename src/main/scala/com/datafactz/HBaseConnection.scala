package com.datafactz

import java.sql.{Connection, DriverManager}
import java.util.{Calendar, List}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import scala.collection.mutable.ListBuffer

/**
  * @author DataFactZ
  *
  *         Implementation of HBase data retrieval methods for Patient Insights Visualization
  */

class HBaseConnection extends HBaseConnectSpec{

  case class rxRecord(age: Int, gender: String, YearMonth: String, svcDt: String, mktedProdNm: String, daysSupplyCnt: Int, pharmacyId: Int, ndcCd: BigInt)
  case class dateRecord(YearMonth: String, svcDt: String, mktedProdNm: String, daysSupplyCnt: Int, pharmacyId: Int, ndcCd: BigInt) extends Ordered[dateRecord] {
    def compare(that: dateRecord): Int = this.svcDt compare that.svcDt
  }
  case class monthRecord(dateRecords: List[dateRecord])

  // connect to the database named "mysql" on the localhost
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://10.1.50.82/ImsPocData"
  val username = "10.1.50.128" // vpn username
  //val username = "nikhil"
  val password = "hadoop"
  var connection: Connection = null

  var prettyPatientJson: String = ""

  // helper method for getPatientHistory
  def getPlottingDates(mappedHistory: scala.collection.immutable.List[(String, scala.collection.immutable.List[(BigInt, scala.collection.immutable.List[dateRecord])])], svcDate: String, ndcCd: BigInt): String = {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
      //val formatter = DateTimeFormat.forPattern("MM/dd/yyyy");
      val dt = formatter.parseDateTime(svcDate);
      val mycal = dt.toGregorianCalendar

      val dateBuilder = new ListBuffer[String]()
      val indexBuilder = new ListBuffer[String]()

      var targetYearMonth: String = ""

      // below code block is only for yyyy-MM-dd date format
      if (dt.getMonthOfYear < 10) {
        targetYearMonth = dt.getYear.toString + "-0" + dt.getMonthOfYear.toString
      }
      else {
        targetYearMonth = dt.getYear.toString + "-" + dt.getMonthOfYear.toString
      }

      val daysInMonth = mycal.getActualMaximum(Calendar.DAY_OF_MONTH);
      var numEvents: Int = 1
      var inc: Int = 1

      // calculate numEvents for given svcDt & ndcCd
      mappedHistory.find(e => e._1 == targetYearMonth)
      var i = 0
      for (i <- 0 to mappedHistory.length - 1) {
        indexBuilder += mappedHistory(i)._1.toString()
        // found Month
        if (mappedHistory(i)._1.toString() == targetYearMonth) {
          val ndcMonthMap = mappedHistory(i)._2.map {
            case (x, y) => (x, y.length)
          }.toMap

          numEvents = ndcMonthMap(ndcCd)
        }
      }
      var currDate = 0

      if (numEvents == 1) {
        inc = daysInMonth / (numEvents + 1)
        for (i <- 0 to numEvents - 1) {
          val dateMember = new DateTime(dt.getYear, dt.getMonthOfYear, inc + currDate, 0, 0, 0, 0)
          dateBuilder += dateMember.toString
          currDate = currDate + inc
        }
      }
      else {
        inc = daysInMonth / numEvents
        for (i <- 0 to numEvents - 1) {
          val dateMember = new DateTime(dt.getYear, dt.getMonthOfYear, inc/2 + currDate, 0, 0, 0, 0)
          dateBuilder += dateMember.toString
          currDate = currDate + inc
        }
      }

      val datesList = dateBuilder.toList.map { x =>
        new DateTime(x)
      }

      val offsets = datesList.map( x =>
        ((x.getDayOfMonth - dt.getDayOfMonth), math.abs(x.getDayOfMonth - dt.getDayOfMonth))
      ).sortBy(_._2)

      val res = new DateTime(dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth + offsets(0)._1, 0, 0, 0, 0).toString()

      res
    }

  /*
  *   Actual implementation of the methods specified in HBaseConnectSpec trait
  */

  //returns a JSON string a patient's Rx, Diag and Prc timeline data given a patientId and date range
  def getPatientHistory(patientId: BigInt, svcFrDt: String, svcToDt: String) = {
    var rxHistoryBuilder = new ListBuffer[rxRecord]()
    var ndcBuilder = new ListBuffer[BigInt] ()
    var dateBuilder = new ListBuffer[dateRecord] ()

    // set query strings here
    val getRecords = "select patientId, age, gender, DATE_FORMAT(svcDate,'%Y-%m') as YearMonth, svcDate, mktedProdNm, daysSupplyCnt, pharmacyId, ndcCd  from rxData where patientId = " + patientId + " and svcDate between '" +  svcFrDt + "' and '"  + svcToDt + "' group by DATE_FORMAT(svcDate,'%Y%m'), ndcCd order by DATE_FORMAT(svcDate,'%Y%m'), svcDate;"
    val getNdcCodes = "select distinct ndcCd from rxData order by ndcCd asc;"

    try {
      // make the connection
      Class.forName(driver).newInstance()
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()

      val rs = statement.executeQuery(getNdcCodes)
      while ( rs.next() ) {
        ndcBuilder += rs.getBigDecimal("ndcCd").toBigInteger
      }

      val resultSet = statement.executeQuery(getRecords)
      while ( resultSet.next() ) {
        var record = new rxRecord(
          resultSet.getInt("age"),
          resultSet.getString("gender"),
          resultSet.getString("YearMonth"),
          resultSet.getString("svcDate"),
          resultSet.getString("mktedProdNm"),
          resultSet.getInt("daysSupplyCnt"),
          resultSet.getInt("pharmacyId"),
          resultSet.getBigDecimal("ndcCd").toBigInteger)
        rxHistoryBuilder += record
      }
    } catch {
      case e: Throwable => "JDBC Connection ERROR: " + e.printStackTrace
    }

    connection.close()

    val ndcList = ndcBuilder.toList
    val rxHistory = rxHistoryBuilder.toList

    var i = 0;

    for (i <- i to rxHistory.length-1) {
      var dr = new dateRecord(rxHistory(i).YearMonth, rxHistory(i).svcDt, rxHistory(i).mktedProdNm, rxHistory(i).daysSupplyCnt, rxHistory(i).pharmacyId, rxHistory(i).ndcCd)
      dateBuilder += dr
    }

    val dateList = dateBuilder.toList
    val dateHistory = dateList.groupBy(w => w.YearMonth).map {
      case(x, y) => (x, y.groupBy(z => z.ndcCd).toList)
    }

    val mappedHistory = dateHistory.toList.sortBy(_._1)

    val patientJson =
      ("patientId" -> patientId) ~
        ("age" -> rxHistory.last.age) ~
        ("gender" -> rxHistory(0).gender) ~
        ("fromDate" -> svcFrDt) ~
        ("toDate" -> svcToDt) ~
        ("rxHistory" ->
          mappedHistory.map { x =>
            (x._1 -> x._2.map{ y =>
              (y._1.toString -> y._2.map { z =>
                (("svcDt" -> z.svcDt) ~
                  ("plottingDates" -> getPlottingDates(mappedHistory, z.svcDt, z.ndcCd)) ~
                  ("ndcCd" -> z.ndcCd) ~
                  ("mktedProdNm" -> z.mktedProdNm) ~
                  ("daysSupplyCnt" -> z.daysSupplyCnt) ~
                  ("pharmacyId" -> z.pharmacyId))
              })
            })
          })

    prettyPatientJson = pretty(render(patientJson))
    prettyPatientJson
  }

  //returns a JSON string of all markets/diagnosis/procedures and corresponding products/diagnosis type/procedure types

  //returns a JSON string of a list of patient attributes given a ndc_cd, stateCd and date range
  def getPatientList(state_cd: String, ndc_cd: BigInt, fromDt :String, toDt: String): String = {
    var patientsBuilder = new ListBuffer[patientRecord]()

    val getPatientsList = "select distinct rxData.patientId, stateCd, payType, dxData.priSpclCd as Speciality, age, gender from rxData join dxData on rxData.patientId = dxData.patientId where rxData.patientId != 0 and ndcCd = " + ndc_cd + " and stateCd = '" + state_cd + "'  and svcDate between '" +  fromDt + "' and '"  + toDt + "' group by patientId having Speciality != '';"

    case class patientRecord(patientId: BigInt, payType: Int, speciality: String, age: Int, gender: String)

    try {
      // make the connection
      Class.forName(driver).newInstance()
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()

      val resultSet = statement.executeQuery(getPatientsList)
      while ( resultSet.next() ) {
        var record = new patientRecord(
          resultSet.getBigDecimal("patientId").toBigInteger,
          resultSet.getInt("payType"),
          resultSet.getString("speciality"),
          resultSet.getInt("age"),
          resultSet.getString("gender"))
        patientsBuilder += record
      }
    } catch {
      case e: Throwable => "JDBC Connection ERROR: " + e.printStackTrace
    }

    connection.close()

    val patientsList = patientsBuilder.toList

    val patientListJson =
      (
        patientsList.map { w =>
          (
            ("patientId" -> w.patientId) ~
              ("payType" -> w.payType) ~
              ("speciality" -> w.speciality) ~
              ("age" -> w.age) ~
              ("gender" -> w.gender)
            )
        }
        )

    val prettyPatientsListsJson = pretty(render(patientListJson))
    prettyPatientsListsJson
  }

}
