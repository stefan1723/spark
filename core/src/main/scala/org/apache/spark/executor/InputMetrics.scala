/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}


/**
 * :: DeveloperApi ::
 * Method by which input data was read. Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 * Operations are not thread-safe.
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}

case class InputReadData(
                        locationExecId: String,
                        readMethod: String,
                        cachedBlock: Boolean
                        ) {

}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about reading data from external systems.
 */
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
  private[executor] val _bytesRead = new LongAccumulator
  private[executor] val _recordsRead = new LongAccumulator
  private[executor] val _readTime = new LongAccumulator
  private[executor] val _readParams = new CollectionAccumulator[InputReadData]
  var test = ""

  /**
   * Total number of bytes read.
   */
  def bytesRead: Long = _bytesRead.sum

  /**
   * Total number of records read.
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * Total time needed for reading
   */
  def readTime: Long = _readTime.sum

  def readParams: Seq[InputReadData] = {
    _readParams.value.asScala
  }

  private[spark] def incBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
  private[spark] def incReadTime(v: Long): Unit = _readTime.add(v)
  private[spark] def incReadParams(v: InputReadData): Unit =
    _readParams.add(v)
  private[spark] def setReadParams(v: java.util.List[InputReadData]): Unit =
    _readParams.setValue(v)
  private[spark] def setReadParams(v: Seq[InputReadData]): Unit =
    _readParams.setValue(v.asJava)

  override def toString: String = {
    s"Bytes Read:${bytesRead}, Records Read:${recordsRead}, Read Time:${readTime}," +
      s"Read Params:${readParams.size}"
  }
}
