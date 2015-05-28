package controllers

import play.api._
import play.api.mvc._
import play.api.libs.ws._
import play.api.libs.json._
import play.api.libs.oauth._
import play.api.Play.current
import play.api.libs.iteratee._
import play.api.libs.concurrent.Execution.Implicits._

import org.joda.time.DateTime

import scala.util.{Success, Failure}
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}

import play.modules.reactivemongo.{MongoController, ReactiveMongoPlugin}

import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson._
import reactivemongo.api._



object Application extends Controller with MongoController {

  // DefaulDB is defined in application.conf (query router in case of sharding)
  val defaultDB = ReactiveMongoPlugin.db
  val commentsColl = defaultDB.collection("comments")
  val topicsColl = defaultDB.collection("topics")

  val driver0 = new MongoDriver()
  // Create one connection per replicaset: 0 to N
  val conn0 = driver0.connection(List("localhost:3001"))
  val local0 = conn0.db("local")
  val oplog0 = local0.collection("oplog.rs")
  // more connections to replicasets here...

  val tailableCursor0: Cursor[BSONDocument] = oplog0
    .find(BSONDocument())
    .options(QueryOpts().tailable.awaitData)
    .cursor[BSONDocument]

  val enumerator0 = tailableCursor0.enumerate()

  val iteratee0 = Iteratee.foreach[BSONDocument]{
    data => updateComments(data)
  }

  val tweetIteratee = Iteratee.foreach[String]{
    data => insertTweets(data)
  }

  enumerator0.apply(iteratee0)

  def updateComments(bs: BSONDocument): Unit = {
    Future {
      val o = bs.getAs[BSONDocument]("o").get
      val operation = bs.getAs[String]("op").get
      operation match {
        case "i" => if(o.getAs[String]("source").get == "app" &&
                       bs.getAs[String]("ns").get == "meteor.comments")
                       updateComment(o)
        case _   =>
      }
    }
  }

  def updateComment(obj: BSONDocument): Unit = {
    Future{
      val topic = obj.getAs[BSONObjectID]("topic")
      val text = obj.getAs[String]("text").get
      val objId = obj.getAs[String]("_id").get
      val sentiment = Sentiment.score(Sentiment.clean(text))
      val selector = BSONDocument("_id" -> objId)
      val modifier = BSONDocument(
          "$set" -> BSONDocument(
            "sentiment" -> sentiment._1,
            "keywords" -> Sentiment.words(Sentiment.clean(text))))
      commentsColl.update(selector, modifier)
      //reactiveUpdateTopic(topic, sentiment._1)
    }
  }


  case class TweetTopic(var id: BSONObjectID, var avg: Double,
                        var total: Double, var pp: Double, var p: Double,
                        var neu: Double, var n: Double, var nn: Double)

  //var topicsId = Map[String,BSONObjectID]()
  //topicsId += topic -> id
  var tweetsMap = Map[String, TweetTopic]()
  var topicsMap = Map[String,TweetTopic]()
  def setTweetTopic(topic: String, id: BSONObjectID, avg: Double,
                    total: Double, bars: BSONDocument): Unit = {
    val pp  = 1.0//bars.getAs[Double]("excellent").get
    val p   = bars.getAs[Double]("good").get
    val neu = bars.getAs[Double]("neutral").get
    val n   = bars.getAs[Double]("bad").get
    val nn  = bars.getAs[Double]("terrible").get
    tweetsMap += topic -> TweetTopic(id, avg, total, pp, p, neu, n, nn)
  }

  def insertTweets(str: String): Unit = {
    Future{
      if(str.contains("""Easy there, Turbo.
                      Too many requests recently.
                      Enhance your calm."""))
        {
         println(str)
         track(" ")
        }
      val json = Json.parse(str)
      insertTweet(json)
      println((json \ "text").as[String])
    }
  }

  def insertTweet(json: JsValue): Unit = {
    val text = (json \ "text").as[String]
    val userName = (json \ "user" \ "screen_name").as[String]
    val sentiment = Sentiment.score(Sentiment.clean(text))
    Future{
      if(sentiment._2 == true){
        for(tweetTopic <- tweetsMap){
          if(text contains tweetTopic._1){
            commentsColl.insert(BSONDocument(
              "_id" -> BSONObjectID.generate.stringify,
              "topic" -> tweetTopic._2.id,
              "text" -> text,
              "author" -> userName,
              "posted" -> BSONDateTime(new DateTime().getMillis),
              "sentiment" -> sentiment._1,
              "keywords" -> Sentiment.words(Sentiment.clean(text)),
              "source" -> "twitter"))
            reactiveTweetTopic(tweetTopic._1, sentiment._1)
          } //end inner if
        } // end for
      } // end outer if
    } // end Future
  }

  def reactiveTweetTopic(topic: String, sentiment: Double): Unit = {
    Future{
      var incPP,incP, incNEU, incN, incNN = 0
      sentiment match {
        case  2.0 => incPP  = 1
        case  1.0 => incP   = 1
        case  0.0 => incNEU = 1
        case -1.0 => incN   = 1
        case -2.0 => incNN  = 1
      }
      val selector = BSONDocument("_id" -> tweetsMap(topic).id)
      val modifier = BSONDocument(
          "$inc" -> BSONDocument(
            "total"-> 1,
            "bars.excellent" -> incPP,
            "bars.good"      -> incP,
            "bars.neutral"   -> incNEU,
            "bars.bad"       -> incN,
            "bars.terrible"  -> incNN))
      topicsColl.update(selector, modifier)
    println(tweetsMap)
    }
  }

  def upsertTweetTopic(topic: String): Unit = {
    Await
    .ready(topicsColl.find(BSONDocument("name" ->BSONString("#" + topic))).one,
                          Duration.Inf)
    .onSuccess{
      case d => d match{
              case None => {
                            val id = BSONObjectID.generate
                            val bars = BSONDocument(
                              "excellent" -> 0.0,
                              "good"      -> 0.0,
                              "neutral"   -> 0.0,
                              "bad"       -> 0.0,
                              "terrible"  -> 0.0)
                            topicsColl.insert(BSONDocument(
                              "_id"       -> id,
                              "name"      -> BSONString("#" + topic),
                              "isActive"  -> 1,
                              "creator"   -> "ReactiveTwitter",
                              "timestamp" -> BSONDateTime(new DateTime()
                                                          .getMillis),
                              "avgSen" -> 0.0,
                              "total"  -> 0.0,
                              "bars"   -> bars))
                            setTweetTopic(topic, id, 0.0, 0.0, bars)
                           }
              case Some(bs) =>{
                               val id = bs.getAs[BSONObjectID]("_id").get
                               val avg = bs.getAs[Double]("avgSen").get
                               val total = bs.getAs[Double]("total").get
                               val bars = bs.getAs[BSONDocument]("bars").get
                               setTweetTopic(topic, id, avg, total, bars)
                              }
                           }
              }
  }


  def track(term: String) = Action { request =>
    tweetsMap = Map[String, TweetTopic]()
    val terms = term.split(",").toList
    terms.foreach(upsertTweetTopic _)
    while(terms.length != tweetsMap.size){
    }
    trackTerms(term)
    Ok("got it")
  }

  def trackTerms(term: String): Unit = {
    val tweetEnumeratee = Enumeratee
                          .map[Array[Byte]](bytes => new String(bytes, "UTF-8"))
    Iteratee.flatten(
    WS.url("https://stream.twitter.com/1.1/statuses/filter.json?track=" + term)
    .sign(OAuthCalculator(Twitter.KEY, Twitter.TOKEN))
    .get((_: (WSResponseHeaders)) => tweetEnumeratee &> tweetIteratee))
    .run
}

/*
  val statsEnumerator = Enumerator.fromCallback{ () =>
    Promise.timeout(Some(true), 1000 milliseconds)
  }

  val statsIteratee = Iteratee.foreach[Boolean](b =>
  if(b) updateStats())

  def updatStats = {
    Future{

    }
  }*/

  def index = Action {
    println("INDEX OK")
    Ok("Your new application is ready.")
  }

}
