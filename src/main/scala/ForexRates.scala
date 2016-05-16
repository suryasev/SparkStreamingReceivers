package com.gofundme.databythebay

import scala.io.Source


//EUR/USD,1462567497620,1.14,007,1.14,014,1.13864,1.14810,1.14039

/**
  * Full set of return values from the TrueFX API
  *
  * @param currencyCode
  * @param timestamp
  * @param bigBidFigure
  * @param bidPoints
  * @param offerBigFigure
  * @param offerPoints
  * @param high
  * @param low
  * @param open
  */
case class FullForexRate(currencyCode: String, timestamp: Long, bigBidFigure: Float, bidPoints: Int, offerBigFigure: Float,
                      offerPoints: Int, high: Float, low: Float, open:Float)

case class ForexRate(timestampInMinutes: Long, rate: Float, currencyCode: String) extends Serializable {
  def maxRate(other:ForexRate): ForexRate = if (this.rate > other.rate) this else other
}

object ForexRates {
  def fromAPIResult(row:String, delimiter:String=",") = {
    val splitRow = row.split(delimiter)
    new ForexRate(splitRow(1).toLong / (60 * 1000), splitRow(2).toFloat, splitRow(0).split("/")(0))
  }
}

class ForexRatesConnector(currencyCode: String = "EUR/USD") {
  val url: String = s"http://webrates.truefx.com/rates/connect.html?f=csv&c=${currencyCode}&s=y"
  def simpleGet: ForexRate = ForexRates.fromAPIResult(Source.fromURL(url).mkString)
}

//TODO: Hardened Connector checking for timeouts and bad results