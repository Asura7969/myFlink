package com.myFlink.project.watermarker

import com.myFlink.project.constants.Constants._
import com.myFlink.project.bean.ComputeResult
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class BoundedLatenessWatermarkAssigner(allowLateness: Int) extends AssignerWithPeriodicWatermarks[ComputeResult] {
  private var maxTimestamp = -1L

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimestamp - allowLateness * 1000L)
  }

  override def extractTimestamp(t: ComputeResult, l: Long): Long = {
    val timestamp = t.metaData(FIELD_TIMESTAMP_INTERNAL).asInstanceOf[Long]
    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }
    timestamp
  }
}
