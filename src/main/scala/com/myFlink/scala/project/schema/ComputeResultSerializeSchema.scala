package com.myFlink.scala.project.schema

import com.alibaba.fastjson.JSON
import com.myFlink.scala.project.bean.ComputeResult
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class ComputeResultSerializeSchema (topic: String) extends KeyedSerializationSchema[ComputeResult] {
  override def serializeValue(t: ComputeResult): Array[Byte] = {
    JSON.toJSON(t).toString.getBytes
  }

  override def serializeKey(t: ComputeResult): Array[Byte] = {
    t.key.getBytes
  }

  override def getTargetTopic(t: ComputeResult): String = topic
}
