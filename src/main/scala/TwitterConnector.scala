package com.gofundme.databythebay

import twitter4j._
import twitter4j.conf.ConfigurationBuilder

import collection.JavaConverters._

case class Tweet(twitterId: Long, screenName: String, text: String)

object Tweet {
  def fromTwitterStatus(ts: Status): Tweet = Tweet(ts.getId, ts.getUser.getScreenName, ts.getText)
}

class TwitterConnector {
  val cb = new ConfigurationBuilder()

  //Consumer and access tokens set through ENV vars

  val tf = new TwitterFactory(cb.build())
  val twitter = tf.getInstance
  val user = twitter.verifyCredentials

  def getMentions(sinceId: Long = -1L): Seq[Tweet] =
    twitter
      .getMentionsTimeline(new Paging(sinceId))
      .asScala
      .map(Tweet.fromTwitterStatus)

  def searchTweets(q:String, sinceId: Long = -1L): Seq[Tweet] = {
    val query = new Query(q)
    query.setSinceId(sinceId)

    twitter
      .search(query)
      .getTweets
      .asScala
      .map(Tweet.fromTwitterStatus)
  }
}

class TwitterConnectorWithFakeSharding(val numberShards: Int) extends TwitterConnector {
  def getShardedMentions(shard: Int, sinceId: Long = 0L) = {
    require(shard >= 0 && shard < numberShards, "Shard number needs to be between 0 and numberShards")
    getMentions(sinceId).filter(_.twitterId % numberShards == shard)
  }
}
