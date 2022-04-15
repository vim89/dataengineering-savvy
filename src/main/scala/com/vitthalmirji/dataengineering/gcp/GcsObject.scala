package com.vitthalmirji.dataengineering.gcp

import com.google.cloud.storage.Blob
import com.vitthalmirji.dataengineering.constants.StringConstants.FORWARD_SLASH
import com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator

/**
 * [[com.vitthalmirji.dataengineering.gcp.GcsObject]] case class representation of Any GCS Object / URI
 *
 * @param projectId  Google project ID - System.getenv("GOOGLE_CLOUD_PROJECT")
 * @param bucketName GCS Bucket Name
 * @param objectName GCS Object Name
 */
case class GcsObject(projectId: Option[String], bucketName: String, objectName: Option[String]) extends Serializable

/**
 * Companion object of [[com.vitthalmirji.dataengineering.gcp.GcsObject]]
 */
object GcsObject extends Serializable {

  /**
   * Implicit class [[com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectConversions]]
   * that represents string as organized GCS Object
   *
   * @param gcsURI For example: gs://bucket/dir1/dir2/file.txt
   */
  implicit class GcsObjectConversions(gcsURI: String) {
    val gcsUri: String = gcsURI.trim

    /**
     * [[com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectConversions#toGcsObject()]]
     * Separates GCS URI String into organized parts viz. Bucket, Object/Blob (s)
     *
     * @return [[com.vitthalmirji.dataengineering.gcp.GcsObject]]
     */
    def toGcsObject: GcsObject = {
      val tokens = if (gcsUri.startsWith("gs://")) {
        gcsUri.substring(5).split(FORWARD_SLASH, 2)
      } else {
        gcsUri.split(FORWARD_SLASH, 2)
      }

      if (tokens.isEmpty) {
        throw new IllegalArgumentException(s"Invalid GCS URI $gcsUri")
      } else if (tokens.tail.isEmpty) {
        GcsObject(None, tokens.head, None)
      } else {
        GcsObject(None, tokens.head, Some(tokens.tail.mkString(FORWARD_SLASH)))
      }
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectToBlob]]
   * GcsObject to Blob converter
   *
   * @param gcsObject gcs URL as object type [[com.vitthalmirji.dataengineering.gcp.GcsObject]]
   */
  implicit class GcsObjectToBlob(gcsObject: GcsObject) {
    /**
     * [[com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectToBlob#toBlob()]]
     *
     * @return Blob typpe from GcsObject
     */
    def toBlob: Blob =
      blob"${gcsObject.bucketName}${gcsObject.objectName.map(FORWARD_SLASH + _.trim).getOrElse(FORWARD_SLASH)}"
  }
}
