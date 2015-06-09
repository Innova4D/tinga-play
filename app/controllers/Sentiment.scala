package controllers

import play.api._
import play.api.mvc._
import play.api.Play.current

import scala.util.matching.Regex

import tinga.sentiment._

object Sentiment extends Controller{
    val s = new Sentiment("es")

    def score(str: String): (Double, Boolean) = {
      //(comment:String, sentiment:Double, flag:String, intensity:Int)
      val tuple = s.globalParagraphScore(s.sentiment(str, false))
      val sentiment = tuple._2
      val intensity = tuple._4
      val valid = tuple._3 match {
                            case "no-sentiment" => false
                            case _              => true
      }
      (sentiment, valid)
    }

    def words(str: String): List[String] = {
      val hashtag = new Regex("#\\S+")
      val newStr = hashtag.replaceAllIn(str, m => "")
      s.wordCloud(newStr).toList.filter(x => x!="")
                         .map(y => y.replace("\n"," ").trim)
                         .filter(z => z!="")
    }

    def clean(str: String): String = {
      val mentions = new Regex("@\\S+")
      val retweet = new Regex("RT ")
      val link = new Regex("https?\\S+")
      mentions.replaceAllIn(
        retweet.replaceAllIn(
          link.replaceAllIn(str, m => "")
                               , m => "")
                               , m => "")
    }


}
