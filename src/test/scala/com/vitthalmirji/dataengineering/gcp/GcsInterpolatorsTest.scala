package com.vitthalmirji.dataengineering.gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, StorageOptions}
import com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator
import com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class GcsInterpolatorsTest extends AnyFunSuite with BeforeAndAfterAll {

  ignore("blobId") {
    val blobId = blobId"gs://ad_chnl_perf_ds_store/test-dir/test_file.txt"
    assert(blobId.getBucket.equals("ad_chnl_perf_ds_store"))
    val content = StorageOptions.getDefaultInstance.getService.get(blobId).getContent()
    assert(content != null)
    assert(content.length > 1)

    val blobId2 = blobId"gs://ad_chnl_perf_ds_store/ww_chnl_perf_app.db/"
    assert(blobId2.toGsUtilUri.equals("gs://ad_chnl_perf_ds_store/ww_chnl_perf_app.db/"))
  }

  ignore("blob with gs://") {
    val blob = blob"gs://ad_chnl_perf_ds_store/test-dir/test_file.txt"
    assert(blob.isFile)
    val blob2 = blob"gs://ad_chnl_perf_ds_store/test-dir/"
    assert(!blob2.isFile)
  }

  ignore("blobs") {
    val blobs: Page[Blob] = blobs"gs://ad_chnl_perf_ds_store/test-dir/"
    assert(blobs.iterateAll().asScala.size.equals(3))
    assert(blobs.iterateAll().asScala.count(!_.isFile).equals(1))
    blobs.iterateAll().forEach(println)
  }

  override protected def afterAll(): Unit = {}
}
