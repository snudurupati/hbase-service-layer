package com.datafactz

/**
 * @author DataFactZ
 *
 *         Specification of HBase data retrieval methods for Patient Insights Visualization
 */

  trait HBaseConnectSpec {

    //returns a JSON string of a list of patient attributes given a ndc_cd, stateCd and date range
    def getPatientList(state_cd: String, ndc_cd: BigInt, fromDt :String, toDt: String): String

    //returns a JSON string a patient's Rx, Diag and Prc timeline data given a patientId and date range
    def getPatientHistory(patientId: BigInt, fromDt :String, toDt: String): String
  }
