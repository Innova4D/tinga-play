package controllers

import play.api._
import play.api.mvc._
import play.api.libs.ws._
import play.api.libs.oauth._
import play.api.Play.current
import play.api.libs.iteratee._


object Twitter extends Controller{

  val KEY = ConsumerKey("SJvLmsowFeSJze1wEai0oWFw9",
                        "5mMkw6aue8W6ZkxvO5LdUlg5Qi648zzTB85SDQzJoflWGPTdis")

  var TOKEN = RequestToken("", "")

  def authenticate() = Action(parse.json) { request =>
    val token  = (request.body \ "token").as[String]
    val secret = (request.body \ "secret").as[String]
    TOKEN = RequestToken(token, secret)
    if(token.length == 50 && secret.length == 45)
      Ok("Proper credentials" +  "\n")
    else
      Result(
          header = ResponseHeader(400, Map(CONTENT_TYPE -> "text/plain")),
          body = Enumerator("No credentials found \n".getBytes())
          )
  }

}
