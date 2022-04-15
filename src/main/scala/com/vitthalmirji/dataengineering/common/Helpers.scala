package com.vitthalmirji.dataengineering.common

import com.vitthalmirji.dataengineering.constants.StringConstants.ERROR_CONSTANT
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object Helpers {
  @transient
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * [[com.vitthalmirji.dataengineering.common.Helpers#trySafely(scala.Function0, scala.Option, scala.Function0)]]
   * is heavily generic higher-order function wrapper of Try/Catch block for embedding unsafe code & handle exceptions
   *
   * @param unsafeCodeBlock            Code block to execute that may be unsafe & throws Exception
   * @param errorMessage               message to log if caught
   * @param exceptionHandlingCodeblock Code block to handle caught exception safely
   * @tparam T Generic typed unsafe code block to execution safely
   * @tparam E Generic typed Exception which are subclasses of [[package$#Throwable]]
   * @tparam C generic typed safe code block to handle caught exception it can be finally statements as well
   * @return Returns output of executed unsafe code block if run smooth; safe exception handling code block output or
   *         throws caught exception once again on user's face to handle by themselves
   *         Usage:
   *         val tryResult = trySafely({
   *         1 / 0
   *         }, errorMessage = Some("Cannot divide by zero"), exceptionHandlingCodeblock = Some({"Exception Handling Code Block"}))
   *         assert(tryResult.isLeft)
   *         assert(!tryResult.isRight)
   *         if (tryResult.isRight) {
   *         println(tryResult.right.get)
   *         } else {
   *         println(tryResult.left.get)
   *         assert(tryResult.left.get.equals("Exception Handling Code Block"))
   *         }
   */
  def trySafely[T, E <: Throwable, C](unsafeCodeBlock: => T, errorMessage: Option[String], exceptionHandlingCodeblock: => Option[C] = None): Either[C, T] = {
    val message: String = errorMessage.getOrElse(ERROR_CONSTANT)
    Try {
      unsafeCodeBlock
    } match {
      case Success(codeBlockResult) => Right(codeBlockResult)
      case Failure(exception: Throwable) =>
        Left(
          handleException(
            exception = exception,
            errorMessage = message,
            exceptionHandlingCodeblock = exceptionHandlingCodeblock
          )
        )
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Helpers#handleException(java.lang.Throwable, java.lang.String, scala.Function0)]]
   * It's a higher-order helper function, handles exception by executing user provided code block or throws exception
   *
   * @param exception                  Exception type hierarchy <= [[package$#Throwable]]
   * @param errorMessage               Error message to log
   * @param exceptionHandlingCodeblock generic code block to execute to handle caught exception
   * @tparam E Generic exception type
   * @tparam C Generic typed named function
   * @return Returns output of code block or throws caught exception again
   *         Usage:
   *         val tryResult = Try {
   *         codeBlock
   *         } match {
   *         case Success(codeBlockResult) => Right(codeBlockResult)
   *         case Failure(exception: Throwable) => Left(handleException(exception, message, exceptionHandlingCodebock))
   *         }
   *         }
   *
   */
  private def handleException[E <: Throwable, C](exception: E, errorMessage: String, exceptionHandlingCodeblock: => Option[C]): C = {
    val codeBlockResult = exceptionHandlingCodeblock.map(c => c)
    if (codeBlockResult.isEmpty) {
      logger.error(errorMessage)
      exception.printStackTrace()
      throw exception
    }
    codeBlockResult.get
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Helpers#isMatching(java.lang.String, java.lang.String)]]
   * Uses [[com.vitthalmirji.dataengineering.common.Helpers#extractMatching(java.lang.String, java.lang.String)]] internally
   *
   * @param searchString String to match, extract regex & check if matching value exists
   * @param regex        Any valid regular expression
   * @return Returns if match succeeded
   */
  def isMatching(searchString: String, regex: String): Boolean = {
    val matched = extractMatching(searchString, regex)
    matched.isDefined
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Helpers#extractMatching(java.lang.String, java.lang.String)]]
   *
   * @param matchIn String to match & extract regex
   * @param regex   Any valid regular expression
   * @return Returns first matched substring from given input
   */
  def extractMatching(matchIn: String, regex: String): Option[String] = {
    val pattern = regex.r
    val matchedString = pattern.findFirstIn(matchIn)
    matchedString
  }
}
