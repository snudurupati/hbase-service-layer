package com.datafactz

/**
 * @author DataFactZ
 *
 *         HBase data retrieval methods for Patient Insights Visualization
 */
object HBaseConnection {
  
  def main(args : Array[String]) {
    println( "HBase connection specification" )
  }

  trait HBaseConnectSpec {

    //returns a JSON string of all markets/diagnosis/procedures
    def getSummaryData(code: String): String

    //returns a JSON string of all products/diagnosis type/procedure for a given market/diagnosis/procedure
    def getDetailData(market_id: Int, diag_id: Int, prc_id: Int): String

    //returns a JSON string of counts by groupByKey for a given ndc_cd/diag_cd/prc_cd and date range.
    def getAggregateData(ndc_cd: BigInt, diag_cd: BigInt, prc_cd: BigInt, fromDt :String, toDt: String): String

    //returns a JSON string of all distinct pay types and specialities
    def getFilterData: String

    //returns a JSON string of a list of patient attributes given an ndc_cd, stateCd and date range
    def getPatientList(state_cd: String, ndc_cd: BigInt, fromDt :String, toDt: String): String

  }

}
