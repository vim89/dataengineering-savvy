package com.vitthalmirji.dataengineering.gcp

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Blob.BlobSourceOption
import com.vitthalmirji.dataengineering.common.Helpers.trySafely
import com.vitthalmirji.dataengineering.constants.DateTimeConstants.TZ_UTC
import com.vitthalmirji.dataengineering.constants.StringConstants.{COMMA, FORWARD_SLASH}
import com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator
import com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectConversions
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.TimeZone

/**
 * Singleton Object [[com.vitthalmirji.dataengineering.gcp.ObjectActions]]
 * that represents implicits for [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations]]
 */
object ObjectActions extends Serializable {
  @transient
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * Implicit class [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations]]
   * that has library functions on top of [[com.google.cloud.storage.Blob]] object
   *
   * @param blob works on top of object [[com.google.cloud.storage.Blob]]
   */
  implicit class BlobOperations(blob: Blob) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#getValueAsString()]]
     * converts blob contents to string
     *
     * @return Blob contents to string
     */
    def getValueAsString: String = blob.getContent().map(_.toChar).mkString

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#createdTs(scala.Option)]]
     * fetches created timestamp of Blob
     *
     * @param timeZone Optional timezone parameter if not specified returns time in UTC
     * @return created timestamp of Blob as [[java.time.LocalDateTime]]
     */
    def createdTs(timeZone: Option[TimeZone]): LocalDateTime =
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(blob.getCreateTime),
        timeZone.getOrElse(TZ_UTC).toZoneId
      )

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#updatedDate(scala.Option)]]
     * internally calls [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#updatedTs(scala.Option)]]
     * to fetch last updated timestamp of Blob & converts to plain Date format
     *
     * @param timeZone Optional timezone parameter if not specified returns time in UTC
     * @return last updated date of Blob as [[java.time.LocalDate]]
     */
    def updatedDate(timeZone: Option[TimeZone]): LocalDate = updatedTs(timeZone).toLocalDate

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#updatedTs(scala.Option)]]
     * fetches latest updated timestamp of Blob
     *
     * @param timeZone Optional timezone parameter if not specified returns time in UTC
     * @return last updated timestamp of Blob as [[java.time.LocalDateTime]]
     */
    def updatedTs(timeZone: Option[TimeZone]): LocalDateTime =
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(blob.getUpdateTime),
        timeZone.getOrElse(TZ_UTC).toZoneId
      )

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#isFile()]] checks if object is File
     *
     * @return Boolean true if object is File type else false
     */
    def isFile: Boolean =
      trySafely(unsafeCodeBlock = !blob.getName.endsWith("/"), errorMessage = Some(s"Error accessing blob ${blob.getName}")).merge

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#move(java.lang.String, scala.Option)]] Moves Blob
     * from existing location to given location
     *
     * @param targetBucketName Target bucket name where Object/Blob is to be moved
     * @param targetObjectName Target object name if Object/Blob requires rename/new name after moving
     */
    def move(targetBucketName: String, targetObjectName: Option[String]): Unit = {
      val copyResult = copy(targetBucketName, targetObjectName)
      if (copyResult) remove() else logger.warn(s"Source blob doesn't exists ${blob.getName}")
    }

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#remove()]] Hard deletes the Blob
     *
     * @return Boolean value true = succeeded deleting & false = failed deleting
     */
    def remove(): Boolean =
      trySafely(blob.delete(), Some(s"Error deleting blob after copy ${blob.getName}")).merge

    /**
     * [[com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations#copy(java.lang.String, scala.Option, scala.collection.Seq)]]
     * Copies GCS object / Blob into given location
     *
     * @param targetBucketName  Target bucket name where Object/Blob is to be copied
     * @param targetObjectName  Target object name if Object/Blob requires rename/new name after copying
     * @param blobSourceOptions Extra options during copy
     * @return Boolean value if copy succeeded
     */
    def copy(targetBucketName: String, targetObjectName: Option[String], blobSourceOptions: BlobSourceOption*): Boolean = {
      val copyResult = trySafely(
        if (targetObjectName.isDefined) {
          blob.copyTo(targetBucketName, targetObjectName.get, blobSourceOptions: _*)
        } else {
          blob.copyTo(targetBucketName, blobSourceOptions: _*)
        },
        errorMessage = Some(s"Error copying blob ${blob.getName}")
      )
      if (copyResult.merge.isDone) {
        true
      } else {
        logger.error(s"Blob doesn't exist ${blob.getName}")
        false
      }
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.gcp.ObjectActions#parallelCopy(org.apache.spark.rdd.RDD)]]
   *
   * @param fileLocations RDD of source & destination file locations given as string; then converted to Blob type for copying
   * @return Boolean if all files have copied successfully
   *         Usage:
   *
   */
  def parallelCopy(fileLocations: RDD[(String, String)]): Boolean = {
    val copyResult = fileLocations.mapPartitions(partitionAttribute => {
      partitionAttribute.map { case (source, destination) =>
        logger.warn(s"Attempting to copy $source to $destination")
        val sourceBlob = blob"$source"
        val target = destination.toGcsObject
        val copyResult = sourceBlob.copy(target.bucketName, target.objectName)
        (sourceBlob, copyResult)
      }
    })
    val failedCopyBlobs = copyResult.filter(_._2.equals(false))
    val result = failedCopyBlobs.take(1).isEmpty
    if (!result) {
      logger.error("Error copying blobs: " + failedCopyBlobs.collect().map(b => b._1.getBucket + FORWARD_SLASH + b._1.getName).mkString(COMMA))
    }
    result
  }
}
