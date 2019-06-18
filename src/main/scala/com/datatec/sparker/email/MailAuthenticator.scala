package com.datatec.sparker.email

import javax.mail.PasswordAuthentication
import javax.mail.Authenticator

class MailAuthenticator (username:String,password:String) extends Authenticator{

  override  def  getPasswordAuthentication():PasswordAuthentication = {
    new PasswordAuthentication(username, password)
  }
  
  def getUserName = username
  
  def getPassword = password
}