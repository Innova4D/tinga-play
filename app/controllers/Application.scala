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

  def insertTweets(str: String): Unit = {
    Future{
      //val str = new String(chunk, "UTF-8")
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

  enumerator0.apply(iteratee0)

  case class TweetTopic(  var id: BSONObjectID,
                          var avg: Double,
                          var pp: Int,
                          var p: Int,
                          var neu: Int,
                          var n: Int,
                          var nn: Int)

  var topicsId = Map[String,BSONObjectID]()
  var tweetsMap = Map[String, TweetTopic]()
  var topicsMap = Map[String,TweetTopic]()
  def setTopicId(topic: String, id: BSONObjectID, avg: Double,
                 pp: Int, p: Int, neu: Int, n: Int, nn: Int): Unit = {
    topicsId += topic -> id
    val tweetTopic = TweetTopic(id, avg, pp, p, neu, n, nn)
    tweetsMap += topic -> tweetTopic
  }

  def insertTweet(json: JsValue): Unit = {
    val text = (json \ "text").as[String]
    val userName = (json \ "user" \ "screen_name").as[String]
    val sentiment = Sentiment.score(text)
    Future{
      if(sentiment._2 == true){
        for(topicId <- topicsId){
          if(text contains topicId._1){
            commentsColl.insert(BSONDocument(
              "_id" -> BSONObjectID.generate.stringify,
              "topic" -> topicId._2,
              "text" -> text,
              "author" -> userName,
              "posted" -> BSONDateTime(new DateTime().getMillis),
              "sentiment" -> sentiment._1,
              "keywords" -> Sentiment.words(text),
              "source" -> "twitter"))
            //reactiveTopic(topicId._2, sentiment._1)
          } //end inner if
        } // end for
      } // end outer if
    } // end Future
  }

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
      val sentiment = Sentiment.score(text)
      val selector = BSONDocument("_id" -> objId)
      val modifier = BSONDocument(
          "$set" -> BSONDocument(
            "sentiment" -> sentiment._1,
            "keywords" -> Sentiment.words(text)))
      commentsColl.update(selector, modifier)
      //reactiveTopic(topic, sentiment._1)
    }
  }

  /*def reactiveTopic(topic: BSONObjectID, sentiment: Double): Unit = {
    Future{
      val selector = BSONDocument("_id" -> topic)
      val modifier = BSONDocument
    }
  }*/

  def upsertTopic(topic: String): Unit = {
    Await
    .ready(topicsColl.find(BSONDocument("name" -> topic)).one, Duration.Inf)
    .onSuccess{
      case d => d match{
              case None => {
                            val id = BSONObjectID.generate
                            topicsColl.insert(BSONDocument(
                            "_id" -> id,
                            "name" -> topic,
                            "isActive" -> 1,
                            "creator" -> "ReactiveTwitter",
                            "timestamp" -> BSONDateTime(new DateTime()
                                                        .getMillis)
                            ))
                            setTopicId(topic, id,0.0 , 0, 0, 0, 0, 0)
                           }
              case Some(bs) =>{
                               val id = bs.getAs[BSONObjectID]("_id").get
                               val avg = bs.getAs[Double]("avgSen").get
                               val pp = bs.getAs[Double]("excellent").get.toInt
                               val p = bs.getAs[Double]("good").get.toInt
                               val neu = bs.getAs[Double]("neutral").get.toInt
                               val n = bs.getAs[Double]("bad").get.toInt
                               val nn = bs.getAs[Double]("terrible").get.toInt
                               setTopicId(topic, id, avg, pp, p, neu, n, nn)
                              }
                           }
              }
  }


  def track(term: String) = Action { request =>
    term.split(",").toList.foreach(upsertTopic _)
    trackTerms(term)
    Ok("got it")
  }

  def trackTerms(term: String): Unit = {
    val tweetEnumeratee = Enumeratee
                          .map[Array[Byte]](bytes => new String(bytes, "UTF-8"))
    println()
    println(tweetsMap)
    Iteratee.flatten(
    WS.url("https://stream.twitter.com/1.1/statuses/filter.json?track=" + term)
    .sign(OAuthCalculator(Twitter.KEY, Twitter.TOKEN))
    .get((_: (WSResponseHeaders)) => tweetEnumeratee &> tweetIteratee))
    .run
}

  def index = Action {
    println("INDEX OK")
    Ok("Your new application is ready.")
  }

}
