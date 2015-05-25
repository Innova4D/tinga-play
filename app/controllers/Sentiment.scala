package controllers

import play.api._
import play.api.mvc._
import play.api.Play.current

import tinga.sentiment._

object Sentiment extends Controller{
    val s = new Sentiment("es")

    def score(str: String): Double = {
      s.globalParagraphScore(s.sentiment(str))._2
    }

    def words(str: String): List[String] = {
      s.wordCloud(str).toList
    }

}
