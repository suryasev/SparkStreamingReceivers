package com.gofundme.databythebay

import com.gofundme.databythebay.{Tweet, TwitterConnector}
import com.redis._
import com.redis.serialization.Parse.Implicits._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver.Receiver

class ReliableTwitterReceiver(val inputQuery: String)
  extends Receiver[Tweet](StorageLevel.MEMORY_AND_DISK_SER_2) with Logging {

  override def onStart(): Unit = {
    new Thread("Fake Reliable Receiver") {
      override def run() { receive() }
    }.start()
  }

  def receive() = {
    val twitterConnector = new TwitterConnector
    val redisClient = new RedisClient("localhost", 6379)

    var lastId: Long = redisClient.get[Long]("lastId").getOrElse(-1L)
    val cachedMentions = new ArrayBuffer[Tweet]

    while(!isStopped) {
      val mentions = twitterConnector.searchTweets(inputQuery, lastId)
      mentions.foreach(cachedMentions += _)

      if (cachedMentions.size > 0) {
        store(cachedMentions)
        lastId = cachedMentions.map(_.twitterId).max
        redisClient.set("lastId", lastId)
        cachedMentions.clear()
      }

      Thread.sleep(10000)
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

  def incrementToRedis(implicit redisClient: RedisClient, keyPrefix: String = "word"): Unit =
    counts.foreach {
      case (word: String, count: Int) => redisClient.incrby(s"${keyPrefix}_$word", count)
    }

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


object TwitterStreamWithReliableReceiver extends App {
  val conf = new SparkConf().setAppName("TwitterStreamer")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(20))

  implicit val redisClient = new RedisClient("localhost", 6379)

  val stream = ssc.receiverStream(new ReliableTwitterReceiver("#databythebay"))

  val wordCounts = stream.map(WordCount.fromTweet)

  val commonWords = wordCounts.reduce(_ reduceWordCounts _)

  commonWords.foreachRDD(_.foreach(_.incrementToRedis))

  stream.window(Minutes(60))

  ssc.start()
  ssc.awaitTermination()

}
