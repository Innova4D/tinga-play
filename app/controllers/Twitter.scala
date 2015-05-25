package controllers

import play.api._
import play.api.mvc._
import play.api.libs.ws._
import play.api.libs.oauth._
import play.api.Play.current

object Twitter extends Controller{

  val KEY = ConsumerKey("SJvLmsowFeSJze1wEai0oWFw9",
                        "5mMkw6aue8W6ZkxvO5LdUlg5Qi648zzTB85SDQzJoflWGPTdis")

  var TOKEN = RequestToken("", "")

  def authenticate() = Action(parse.json) { request =>
    val token  = (request.body \ "token").as[String]
    val secret = (request.body \ "secret").as[String]
    TOKEN = RequestToken(token, secret)
    Ok("Proper credentials")
  }

}
