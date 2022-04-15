package com.vitthalmirji.dataengineering.gcp

import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.testing.RemoteStorageHelper
import com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator
import com.vitthalmirji.dataengineering.gcp.GcsObject.GcsObjectConversions
import com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8

class ObjectActionsTest extends AnyFunSuite with BeforeAndAfterAll {
  private val BUCKET = RemoteStorageHelper.generateBucketName
  private val BLOB = "blob"
  private val STRING_CONTENT = "Hello, World!"
  private val CONTENT = STRING_CONTENT.getBytes(UTF_8)
  private val helper: RemoteStorageHelper = RemoteStorageHelper.create
  // private val storage = helper.getOptions.getService
  private val storage = StorageOptions.getDefaultInstance.getService

  override protected def beforeAll(): Unit = {}

  test("GcsObject") {
    val gcsObject = "gs://ad_chnl_perf_ds_store/ww_chnl_perf_app.db/".toGcsObject
    val gcsObject2 = "gs://ad_chnl_perf_ds_store/test-dir/test_file.txt".toGcsObject
    val gcsObject3 = "ad_chnl_perf_ds_store/ww_chnl_perf_app.db/".toGcsObject
    val gcsObject4 = "ad_chnl_perf_ds_store/test-dir/test_file.txt".toGcsObject
    val gcsObject5 = "gs://ad_chnl_perf_ds_store/dir1/dir2/dir3/test_file.txt".toGcsObject
    assert(gcsObject.objectName.get.equals("ww_chnl_perf_app.db/"))
    assert(gcsObject2.bucketName.equals("ad_chnl_perf_ds_store"))
    assert(gcsObject3.objectName.get.equals("ww_chnl_perf_app.db/"))
    assert(gcsObject4.objectName.get.equals("test-dir/test_file.txt"))
    assert(gcsObject5.objectName.get.equals("dir1/dir2/dir3/test_file.txt"))
  }

  ignore("Blob copy") {
    val blob = blob"gs://ad_chnl_perf_ds_store/test-dir/test_file.txt"
    val target = "gs://ad_chnl_perf_ds_store/ww_chnl_perf_stg.db/test_file_copied.txt".toGcsObject
    assert(blob.isFile)
    val result = blob.copy(targetBucketName = target.bucketName, target.objectName)
    assert(result)
    val copiedBlob = target.toBlob
    assert(copiedBlob.isFile)
  }

  override protected def afterAll(): Unit = {}
}
