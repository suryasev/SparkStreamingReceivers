package com.gofundme.databythebay.unreliable

import com.gofundme.databythebay.{Tweet, TwitterConnector}
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver.Receiver

class UnreliableTwitterReceiver(val inputQuery: String) extends Receiver[Tweet](StorageLevel.MEMORY_AND_DISK_SER_2) with Logging {
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def receive() = {
    val twitterConnector = new TwitterConnector
    var lastId = -1L

    while(!isStopped) {
      val mentions = twitterConnector.searchTweets(inputQuery, lastId)
      mentions.foreach(store)
      if (mentions.length > 0)
        lastId = mentions.map(_.twitterId).max
      Thread.sleep(20000)
    }
  }

  override def onStop() = {}
}

class WordCount(val counts: Seq[(String, Int)]) extends Serializable {
  def reduceWordCounts(other_wc: WordCount): WordCount = {
    val aggregateCounts =
      (counts ++ other_wc.counts)
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
    new WordCount(aggregateCounts.toSeq)
  }

  def topWords(n: Int = 5) = counts.sortBy(-1 * _._2).take(n)

  override def toString() = topWords().toString()
}

object WordCount {
    def fromTweet(t: Tweet): WordCount = {
      val counts = t.text
        .split(' ')
        .groupBy(identity)
        .mapValues(_.size)
      new WordCount(counts.toSeq)
    }
}

object Utils {

  def longestTweet(t1: Tweet, t2: Tweet) = if(t1.text.length >= t2.text.length) t1 else t2
}

object UnreliableTwitterReceiverStream extends App {

  val conf = new SparkConf().setAppName("SimpleReceiver")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(20))

  val stream = ssc.receiverStream(new SimpleTwitterReceiver("#datadeaver"))

  stream.foreachRDD(_.foreach(t => println(s"Something something " + t.text)))

  val wordCounts = stream.map(WordCount.fromTweet)

  val commonWords = wordCounts.reduceByWindow(_ reduceWordCounts _, Seconds(600), Seconds(20))
  commonWords.foreachRDD(_.foreach(println))

  ssc.start()
  ssc.awaitTermination()

}
