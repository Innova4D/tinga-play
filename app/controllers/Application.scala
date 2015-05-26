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

  val tweetIteratee = Iteratee.foreach[Array[Byte]]{
    data => insertComments(data)
  }

  def insertComments(chunk: Array[Byte]): Unit = {
    Future{
      val str = new String(chunk, "UTF-8")
      if(str.contains("""Easy there, Turbo.
                      Too many requests recently.
                      Enhance your calm."""))
        {
        println(str)
        track(" ")
        }
      val json = Json.parse(str)
      insertComment(json)
      println((json \ "text").as[String])
    }
  }

  enumerator0.apply(iteratee0)

  var topicId = BSONObjectID("5549020460295bb4f109e086")
  def insertComment(json: JsValue): Unit = {

    val text = (json \ "text").as[String]
    val userName = (json \ "user" \ "screen_name").as[String]
    println(userName)
    //terms.foreach(topic =>
      //if(text contains topic){
        commentsColl.insert(BSONDocument(
          "_id" -> BSONObjectID.generate.stringify,
          "topic" -> topicId,
          "text" -> text,
          "author" -> userName,
          "posted" -> BSONDateTime(new DateTime().getMillis),
          "sentiment" -> Sentiment.score(text),
          "keywords" -> Sentiment.words(text),
          "source" -> "twitter"
        ))
      //})
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
    val text = obj.getAs[String]("text").get
    val objId = obj.getAs[String]("_id").get
    val selector = BSONDocument("_id" -> objId)
    val modifier = BSONDocument(
        "$set" -> BSONDocument(
          "sentiment" -> Sentiment.score(text),
          "keywords" -> Sentiment.words(text)
        ))
    commentsColl.update(selector, modifier)
  }


  def upsertTopic(topic: String): Unit = {
    val doc = topicsColl.find(BSONDocument("name" -> topic))
              .one
              .onSuccess{
                case d => d match{
                  case None => topicsColl.insert(BSONDocument(
                                "name" -> topic,
                                "isActive" -> 1,
                                "creator" -> "ReactiveTwitter",
                                "timestamp" -> BSONDateTime(new DateTime()
                                                            .getMillis)
                                ))
                  case _ =>
                }
              }
  }

  def findTopicId(topic: String): Unit = {
        topicsColl.find(BSONDocument("name" -> topic))
        .one
        .onSuccess{
          case Some(d) => {
            topicId = d.getAs[BSONObjectID]("_id").get
            println(topicId)
            }
        }
  }

  def track(term: String) = Action { request =>
    upsertTopic(term)
    findTopicId(term)
    Iteratee.flatten(
    WS.url("https://stream.twitter.com/1.1/statuses/filter.json?track=" + term)
    .sign(OAuthCalculator(Twitter.KEY, Twitter.TOKEN))
    .get((_: (WSResponseHeaders)) => tweetIteratee)
    ).run
    Ok("got it")
  }

  def index = Action {
    println("INDEX OK")

    Ok("Your new application is ready.")
  }

}
