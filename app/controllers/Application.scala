package controllers

import play.api._
import play.api.mvc._
import play.api.libs.ws._
import play.api.libs.json._
import play.api.libs.oauth._
import play.api.Play.current
import play.api.libs.iteratee._
import play.api.libs.concurrent.Promise
import play.api.libs.concurrent.Akka
import play.api.libs.concurrent.Execution.Implicits._

import org.joda.time.DateTime
import scala.language.postfixOps

import scala.util.{Success, Failure}
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}

import play.modules.reactivemongo.{MongoController, ReactiveMongoPlugin}

import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.core.commands._
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
      val (sentiment, valid) = Sentiment.score(Sentiment.clean(text))
      val selector = BSONDocument("_id" -> objId)
      val modifier = BSONDocument(
          "$set" -> BSONDocument(
            "sentiment" -> sentiment,
            "keywords" -> Sentiment.words(Sentiment.clean(text))))
      commentsColl.update(selector, modifier)
      //reactiveUpdateTopic(topic, sentiment._1)
    }
  }



  var tweetsMap = Map[String,BSONObjectID]()
  def setTweetTopic(topic: String, id: BSONObjectID): Unit = {
    tweetsMap += topic -> id
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
    }
  }

  def insertTweet(json: JsValue): Unit = {
    val text = (json \ "text").as[String]
    val userName = (json \ "user" \ "screen_name").as[String]
    val (sentiment, valid) = Sentiment.score(Sentiment.clean(text))
    Future{
      if(valid == true){
        println(text)
        for(tweetTopic <- tweetsMap){
          if(text contains tweetTopic._1){
            commentsColl.insert(BSONDocument(
              "_id" -> BSONObjectID.generate.stringify,
              "topic" -> tweetTopic._2,
              "text" -> text,
              "author" -> userName,
              "posted" -> BSONDateTime(new DateTime().getMillis),
              "sentiment" -> sentiment,
              "keywords" -> Sentiment.words(Sentiment.clean(text)),
              "source" -> "twitter"))
            reactiveTweetTopic(tweetTopic._1, sentiment)
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
      val selector = BSONDocument("_id" -> tweetsMap(topic))
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
                            //val bars =
                            topicsColl.insert(BSONDocument(
                              "_id"       -> id,
                              "name"      -> BSONString("#" + topic),
                              "isActive"  -> 1,
                              "creator"   -> "Twitter",
                              "timestamp" -> BSONDateTime(new DateTime()
                                                          .getMillis),
                              "avgSen" -> 0.0,
                              "total"  -> 0.0,
                              "bars"   -> BSONDocument(
                                "excellent" -> 0.0,
                                "good"      -> 0.0,
                                "neutral"   -> 0.0,
                                "bad"       -> 0.0,
                                "terrible"  -> 0.0)))
                            setTweetTopic(topic, id)
                           }
              case Some(bs) =>{
                               val id = bs.getAs[BSONObjectID]("_id").get
                               setTweetTopic(topic, id)
                              }
                           }
              }
  }


  def track(term: String) = Action { request =>
    index
    val terms = term.split(",").toList
    tweetsMap = Map[String, BSONObjectID]()
    terms.foreach(upsertTweetTopic _)
    Thread.sleep(100)
    println(tweetsMap)
    trackTerms(term)
    Ok("got it tracking: ***" + terms.mkString("-") + "***\n")
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


  val avgEnumerator = Enumerator.generateM{
    Promise.timeout(Some("Average per Topic"), 1000 milliseconds)
  }

  val avgIteratee = Iteratee.foreach[String](str =>{
         updateReactiveStats(str)})

  avgEnumerator |>> avgIteratee

  def updateReactiveStats(str: String): Unit = {
    for(topic <- tweetsMap){
      /************************** AVERAGE START *************************/
      val commandAvg = BSONDocument(
        "aggregate" -> "comments",
        "pipeline"  -> BSONArray(
          BSONDocument("$match" -> BSONDocument("topic" -> topic._2)),
          BSONDocument("$group" -> BSONDocument(
            "_id" -> "averageGlobal",
            "avgSen" -> BSONDocument("$avg" -> "$sentiment")))))
      val resultAvg = defaultDB.command(RawCommand(commandAvg))
      /* resultAvg
      {
        result: [
          0: {
              _id: BSONString(averageGlobal),
              avgSen: BSONDouble(0.46792035398230086)
            }
        ],
        ok: BSONDouble(1.0)
      }
      */
      resultAvg.onFailure{
        case e => Console.println(e)
      }
      resultAvg.onSuccess{
        case bs => {
          //println(BSONDocument.pretty(bs))
          val resultList = bs.getAs[List[BSONDocument]]("result").toList.flatten
          if(!resultList.isEmpty){
            val avgSen = resultList(0).getAs[Double]("avgSen").get
            val selector = BSONDocument("_id" -> topic._2)
            val modifier = BSONDocument(
              "$set" -> BSONDocument(
                "avgSen" -> avgSen))
            topicsColl.update(selector, modifier)
          }
        }
      }
      /************************** AVERAGE END *************************/
      /****************************************************************/
      /************************ KEYWORDS START ************************/
      val commandWords = BSONDocument(
        "aggregate" -> "comments",
        "pipeline" -> BSONArray(
          BSONDocument("$match" -> BSONDocument("topic" -> topic._2)),
          BSONDocument("$unwind" -> "$keywords"),
          BSONDocument("$group" -> BSONDocument(
            "_id" -> "$keywords",
            "freq" -> BSONDocument("$sum" -> 1))),
          BSONDocument("$sort" -> BSONDocument("freq" -> 1)),
          BSONDocument("$limit" -> 5)))
      val resultWords = defaultDB.command(RawCommand(commandWords))
      /* resultWords
      {
          result: [
            0: {
              _id: BSONString(One Direction),
              freq: BSONInteger(1)
            },
            1: {
              _id: BSONString(hambre),
              freq: BSONInteger(1)
            },
            2: {
              _id: BSONString(hora),
              freq: BSONInteger(1)
            },
            3: {
              _id: BSONString(Plantas Medicinales),
              freq: BSONInteger(1)
            },
            4: {
              _id: BSONString(muñecos chinos),
              freq: BSONInteger(1)
            }
          ],
          ok: BSONDouble(1.0)
        } */

      resultWords.onFailure{
        case e => Console.println(e)
      }
      resultWords.onSuccess{
        case bs => {
          val resultList = bs.getAs[List[BSONDocument]]("result").toList.flatten
          val wordsFreq = resultList
                          .map(b => {
                            val word = b.getAs[String]("_id").get
                            val freq = b.getAs[Int]("freq").get
                            BSONDocument(word -> freq)
                            })
                          .foldLeft(BSONDocument())((acc, item) => acc.add(item))
          val selector = BSONDocument("_id" -> topic._2)
          val modifier = BSONDocument(
            "$set" -> BSONDocument(
              "keywords" -> wordsFreq ))
          topicsColl.update(selector, modifier)
        }
      }
      /************************ KEYWORDS END ************************/
    }
  }

  /*val statsEnumerator = Enumerator.generateM{
    Promise.timeout(Some("Avgerage Stats"), 1000 milliseconds)
  }

  val statsIteratee = Iteratee.foreach[String](str =>{
         updateStats(str)})

  statsEnumerator |>> statsIteratee*/

  def index = Action {
    Ok("Your new application is ready.")
  }

}
