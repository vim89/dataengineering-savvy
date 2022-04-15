package com.vitthalmirji.dataengineering.gcp

import com.google.api.gax.paging.Page
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption}
import com.google.cloud.storage._
import com.vitthalmirji.dataengineering.common.Helpers.trySafely
import com.vitthalmirji.dataengineering.constants.StringConstants.COMMA
import com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectConversions

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/** [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators]] has implicit string interpolators for many use cases
 * [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator]] for storage URI given as string
 */
object GcsInterpolators extends Serializable {

  val API_JSON: String = """""".stripMargin
  val CREDENTIALS_INPUT_STREAM = new ByteArrayInputStream(API_JSON.getBytes(StandardCharsets.UTF_8))

  val STORAGE_SERVICE: Storage =
    StorageOptions.newBuilder().setCredentials(GoogleCredentials.fromStream(CREDENTIALS_INPUT_STREAM)).build().getService

  /** [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator]] for storage URI given as string
   *
   * @param sc String Context for GCS SDK Objects given as string
   */
  implicit class GcsInterpolator(sc: StringContext) extends Serializable {

    /** Creates or Fetches Blob Information of GCS Object
     *
     * @param args gcs URI given as plain string or simple interpolated string
     * @return [[com.google.cloud.storage.BlobId]]
     */
    def blobId(args: Any*): BlobId = blobInfo(args: _*).getBlobId

    /**
     * [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator#blobInfo(scala.collection.Seq)]]
     * Fetches Information of Blob for given string
     *
     * @param args gcs URI given as plain string or simple interpolated string
     * @return
     */
    private def blobInfo(args: Any*): BlobInfo = {
      val totalString = sc.s(args: _*).trim
      val gcsObject: GcsObject = totalString.toGcsObject
      if (gcsObject.objectName.isEmpty) {
        throw new IllegalArgumentException(
          """Path of object is empty. To fetch only bucket object use bucket"" GcsInterpolator""".stripMargin
        )
      } else {
        BlobInfo.newBuilder(gcsObject.bucketName, gcsObject.objectName.get).build()
      }
    }

    /** [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator#blob(scala.collection.Seq)]]
     * Creates or Fetches Blob of GCS Object
     *
     * @param args GCS URI given as plain string or simple interpolated string
     * @return Blob type object
     */
    def blob(args: Any*): Blob = {
      val totalString = sc.s(args: _*)
      val gcsObject = totalString.toGcsObject
      if (gcsObject.objectName.isEmpty) {
        throw new IllegalArgumentException("""Path of object is empty. To fetch only bucket object use bucket"" GcsInterpolator""".stripMargin)
      }
      val tryResult = trySafely(
        unsafeCodeBlock = STORAGE_SERVICE.get(gcsObject.bucketName, gcsObject.objectName.get, BlobGetOption.fields(Storage.BlobField.values(): _*)),
        errorMessage = Some(s"Error fetching blob object $args")
      )
      tryResult.merge
    }

    /** [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator#manyBlobs(scala.collection.Seq)]]
     * String GcsInterpolator Fetches list of Blob objects from given various GCS URI, even from different buckets
     *
     * @param args gcs URI given as plain string or simple interpolated string
     * @return Map of GcsObject & it's list of Blob type objects
     */
    def manyBlobs(args: Any*): Map[GcsObject, Page[Blob]] = {
      val tokens = sc.s(args: _*).trim.split(COMMA)
      tokens.map(gcsUri => {
        gcsUri.toGcsObject -> blobs"$gcsUri"
      }).toMap
    }

    /**
     * [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator#blobs(scala.collection.Seq, com.google.cloud.storage.Storage.BlobListOption[])]]
     * Lists GCS Objects as Blobs
     *
     * @param args            Interpolated string arguments
     * @param blobListOptions Implicit values of Options to suppor while listing Blobs
     * @return Returns Page of Blobs
     */
    def blobs(args: Any*)(implicit blobListOptions: Array[BlobListOption] = Array.empty): Page[Blob] = {
      val totalString = sc.s(args: _*).trim
      val gcsObject = totalString.toGcsObject
      val blobListOptionsWithDefaults = blobListOptions ++ Array(BlobListOption.pageSize(Int.MaxValue))
      val tryResult = trySafely(
        unsafeCodeBlock = gcsObject.objectName.map(obj => {
          STORAGE_SERVICE
            .list(gcsObject.bucketName, Array(prefix"$obj") ++ blobListOptionsWithDefaults: _*)
        }).getOrElse(STORAGE_SERVICE.list(gcsObject.bucketName, blobListOptionsWithDefaults: _*)),
        errorMessage = Some(s"Error fetching blob objects $args")
      )
      tryResult.merge
    }

    /** String GcsInterpolator for GCS URI path object except bucketName
     *
     * @param args path of gcs object except bucketName given as plain string or simple interpolated string
     * @return [[com.google.cloud.storage.Storage.BlobListOption#prefix(java.lang.String)]]
     */
    def prefix(args: Any*): BlobListOption = BlobListOption.prefix(sc.s(args: _*))

    /** [[com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator#buckets(scala.collection.Seq)]]
     * String GcsInterpolator Fetches list of bucket objects
     *
     * @param args Do not pass any arguments to fetch all buckets as List from a GCP Project
     * @return List of Bucket type objects
     */
    def buckets(args: Any*): List[Bucket] =
      trySafely(
        unsafeCodeBlock = STORAGE_SERVICE.list().getValues.asScala.toList,
        errorMessage = Some(s"Error listing buckets")
      ).merge

    /** String GcsInterpolator for GCS bucket
     *
     * @param args bucketName as String
     * @return Bucket object type
     */
    def bucket(args: Any*): Bucket = {
      val totalString = sc.s(args: _*)
      val tryResult = trySafely(
        unsafeCodeBlock = STORAGE_SERVICE.get(totalString),
        errorMessage = Some(s"Error fetching bucket $totalString")
      )
      tryResult.merge
    }
  }
}
