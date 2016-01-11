package com.datafactz

/**
 * @author DataFactZ
 *
 *         Specification of HBase data retrieval methods for Patient Insights Visualization
 */

  trait HBaseConnectSpec {

    //returns a JSON string of all markets/diagnosis/procedures and corresponding products/diagnosis type/procedure types
    def getSummaryData(code: String): String

    //returns a JSON string of counts by groupByKey for a given ndc_cd/diag_cd/prc_cd and date range.
    def getAggregateData(ndc_cd:BigInt, diag_cd: BigInt, prc_cd: BigInt, fromDt :String, toDt: String): String

    //returns a JSON string of all distinct pay types and specialities
    def getFilterData: String

    //returns a JSON string of a list of patient attributes given a ndc_cd, stateCd and date range
    def getPatientList(state_cd: String, ndc_cd: BigInt, fromDt :String, toDt: String): String

    //returns a JSNON string a patient's Rx, Diag and Prc timeline data given a patientId and date range
    def getPatientHistory(patientId: BigInt, fromDt :String, toDt: String): String
  }
