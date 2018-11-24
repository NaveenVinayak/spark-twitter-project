package com.navtweets.analyse


import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Tweet {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }
    val appName = "TwitterData"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret, extendedMode) = args.take(5)
    val filters = args.takeRight(args.length - 4)

    val cb = new ConfigurationBuilder

//    val extndMode = extendedMode.toBoolean
 //   println(extndMode)


    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setTweetModeExtended(true)



    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth),filters)


    val englishTweets = tweets.filter(_.getLang() == "en")

    val b = tweets.map(s => s.getText)
    englishTweets.foreachRDD(rdd => {
      rdd.map(s => {
        //println(s)
        var a = "";
        if(s.getRetweetedStatus != null)
            a = s.getRetweetedStatus().getText
        else
          a = s.getText

          println("")
          println("")
          a


      }).foreach(println)

      /*tweets .saveAsTextFiles("tweets", "json")*/

    })



    ssc.start()
    ssc.awaitTermination()



  }
}
