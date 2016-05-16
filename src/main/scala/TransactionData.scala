package com.gofundme.databythebay

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import sun.reflect.generics.reflectiveObjects.NotImplementedException


case class TransactionDatum(timestampInMinutes: Int, transactionAmount: Float, currencyCode: String)

//object TransactionDatum {
//  def internationalDataStream: DStream[TransactionDatum] = throw NotImplementedException
//
//  def makeTonnesOfMoney(d: RDD[TransactionDatum]): Unit = throw NotImplementedException
//}
//
//object TimeSeriesAnalysis {
//  def expensiveAnalysis[T, U](d: Iterable[T]): Unit = throw NotImplementedException
//}