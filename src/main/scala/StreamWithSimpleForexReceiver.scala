package com.gofundme.databythebay


import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver.Receiver

class SimpleForexReceiver extends Receiver[ForexRate](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    new Thread("Simple Forex Receiver") {
      override def run() { receive() }
    }.start()
  }

  def receive() = {
    val forexConnector = new ForexRatesConnector()

    while(!isStopped) {
      store(forexConnector.simpleGet)
      //Avoid API Rate Limits
      Thread.sleep(10000)
    }
  }

  override def onStop() = {}
}

/**
  * Example Forex Stream from presentation
  */
object SimpleForexStream extends App {

  val conf = new SparkConf().setAppName("SimpleForexReceiver")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val stream = ssc.receiverStream(new SimpleForexReceiver)

  //Calculate the max exchange rate within the last 10 minutes
  val maxRateStream = stream
    .window(Minutes(10))
    .reduce((f1: ForexRate, f2: ForexRate) => if(f1.rate > f2.rate) f1 else f2)

//  maxRateStream.foreachRDD(ForexMagic.somehowMakeLotsOfMoney)
  maxRateStream.foreachRDD(_.foreach(println))

  ssc.start()
  ssc.awaitTermination()

}

/**
  * Example of joining in another large data source
  */
//object JoinExampleReceiver {
//
//  val conf = new SparkConf().setAppName("SimpleForexReceiver")
//  val sc = new SparkContext(conf)
//  val ssc = new StreamingContext(sc, Seconds(10))
//
//  val internationalTransactionData: DStream[TransactionDatum] = TransactionDatum.internationalDataStream
//
//  val forexStream = ssc.receiverStream(new SimpleForexReceiver)
//
//  val distinctForexStream = forexStream
//    .transform(
//    _.map(forex => (forex.timestampInMinutes, forex))
//      .distinct.map(_._2)
//  )
//    .map(forex => ((forex.timestampInMinutes, forex.currencyCode), forex))
//
//  val pairIntTranData = internationalTransactionData.map(d => ((d.timestampInMinutes, d.currencyCode), d))
//
//  val normalizedIntTranData = pairIntTranData
//    .join(distinctForexStream)
//    .map(_._2)
//    .map{ case (d: TransactionDatum, forex: ForexRate) =>
//       new TransactionDatum(d.timestampInMinutes, d.transactionAmount / forex.rate, "USD") }
//
//  normalizedIntTranData.foreachRDD(TransactionDatum.makeTonnesOfMoney(_))
//
//  ssc.start()
//  ssc.awaitTermination()
//}