package delta

import delta.DeltaUpsert.spark

object UpsertEventProcessor {

  def displayEventParquetFile(filename: String): Unit = {
    val path = new java.io.File(s"./tmp/upsert_event_delta_lake/$filename.snappy.parquet").getCanonicalPath
    val df = spark.read.parquet(path)
    df.show(false)
  }

}
