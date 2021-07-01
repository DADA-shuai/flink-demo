package com.puhuilink

case class Metric(host_id:Long,endpoint_id:Long,metric_id:Long,metric_value:Double,collect_time:Long,agent:String,aggregate:String,upload_time:Long,metric_value_str:String) extends Serializable {
//  def this() {
//    this(-1L,-1L,-1L,-1.0,-1L,null,null,-1L,null)
//  }
}
