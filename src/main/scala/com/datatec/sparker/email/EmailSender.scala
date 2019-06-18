package com.datatec.sparker.email

import javax.mail.Address
import javax.mail.Message.RecipientType
import javax.mail.Session
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import javax.mail.PasswordAuthentication
import javax.mail.Transport

private[sparker] class EmailSender(senderUserName:String, senderPwd:String, smtpHost:String) {

  private[this] lazy val session:Session = {
     val  props = System.getProperties()
     props.put("mail.smtp.auth", "true")
     props.put("mail.smtp.starttls.enable", "true")
     props.put("mail.smtp.host", smtpHost)
     props.put("mail.smtp.port", "587")
     Session.getDefaultInstance(props, new javax.mail.Authenticator() {
             override def getPasswordAuthentication() :PasswordAuthentication = {
                new PasswordAuthentication(senderUserName, senderPwd);
            }
          })
  }
  
  def sendMail(recipients:Array[String],subject:String,content:String){
     try {  
	    //session.setDebug(true)
	    // 创建mime类型邮件
	    val message = new MimeMessage(session)
	    // 设置主题
	    message.setSubject(subject)
	    // 设置收件人们
	    val num = recipients.length
	    val addresses = new Array[Address](num)
	    
	    for(i <- 0 until num) addresses(i) = new InternetAddress(recipients(i))
	    
	    message.setRecipients(RecipientType.TO, addresses)
	    // 设置发信人
	    message.setFrom(new InternetAddress(senderUserName,"XZSMonitor"))
	    // 设置邮件内容
	    message.setContent(content.toString(), "text/html;charset=utf-8")
	    message.saveChanges()
	    /*val transport = session.getTransport("smtp");
	    transport.connect(smtpHost, senderUserName, senderPwd);
	    // 发送
	    transport.sendMessage(message,message.getAllRecipients())
	    transport.close()*/
	    Transport.send(message)
	  } catch {  
	  	case ex: Exception => ex.printStackTrace()
	  }
  }
  
  
}

object EmailSender{
  def apply(senderUserName:String, senderPwd:String, smtpHost:String)  = new EmailSender(senderUserName,senderPwd,smtpHost)
}