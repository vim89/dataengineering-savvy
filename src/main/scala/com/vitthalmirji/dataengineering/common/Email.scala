package com.vitthalmirji.dataengineering.common

import org.apache.log4j.Logger
import java.util.Properties
import javax.mail.Message.RecipientType
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Session, Transport}

/**
 * Case class [[com.vitthalmirji.dataengineering.common.Email]]
 * @param from FROM email address
 * @param to List of TO email addresses; Use List("user@example.com")
 * @param subject Email subject
 * @param body Email body
 * @param smtp SMTP Host to use in Session & Transport
 * @param cc List of CC email addresses - Optional ; Use Some(List("user@example.com"))
 * @param bcc List of BCC email addresses - Optional; Use Some(List("user@example.com"))
 */
case class Email(from: String, to: List[String], subject: Option[String], body: Option[String],
                 smtp: String = "smtp-gw1.wal-mart.com", cc: Option[List[String]] = None, bcc: Option[List[String]] = None)

/**
 * Companion object for case class Email [[com.vitthalmirji.dataengineering.common.Email]]
 */
object Email {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * [[com.vitthalmirji.dataengineering.common.Email.EmailActions]] Implicit actions on Email case class,
   * provides functions to chain on Email object
   * @param email of type [[com.vitthalmirji.dataengineering.common.Email]]
   *              val email = Email(from = "user@example.com", to = List("user@example.com"),
   *              subject = "Data quality report for dataframe ww_chnl_perf_app.chnl_perf_item_fact_dly", body = None)
   */
  implicit class EmailActions(email: Email) extends Serializable {
    /**
     * [[com.vitthalmirji.dataengineering.common.Email.EmailActions#send()]]
     * val email = Email(from = "user@example.com", to = List("user@example.com"),
     * subject = "Data quality report for dataframe ww_chnl_perf_app.chnl_perf_item_fact_dly", body = None)
     * email.send
     */
    def send: Unit = {
      if(email.to.isEmpty || email.from.trim.isEmpty || email.smtp.trim.isEmpty) {
        val errorMessage = "FROM, TO email addresses and SMTP values must not be empty"
        logger.error(errorMessage)
        throw new IllegalArgumentException(errorMessage)
      }
      val sessionProperties = new Properties()
      sessionProperties.put("mail.smtp.host", email.smtp) //"smtp-gw1.wal-mart.com"
      val session = Session.getDefaultInstance(sessionProperties)
      val message = new MimeMessage(session)

      message.setFrom(new InternetAddress(email.from))
      email.to.foreach(id => message.addRecipient(RecipientType.TO, new InternetAddress(id)))
      email.cc.foreach(ccIds => ccIds.map(id => message.addRecipient(RecipientType.CC, new InternetAddress(id))))
      email.bcc.foreach(bccIds => bccIds.map(id => message.addRecipient(RecipientType.BCC, new InternetAddress(id))))
      email.subject.foreach(s => message.setSubject(s))
      email.body.foreach(c => message.setContent(c, "text/html"))
      Transport.send(message)
    }
  }
}
