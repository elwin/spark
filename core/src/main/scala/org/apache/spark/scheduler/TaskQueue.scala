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

package org.apache.spark.scheduler

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * `TaskSetManager.resourceOffer`.
 *
 * TaskDescriptions and the associated Task need to be serialized carefully for two reasons:
 *
 * (1) When a TaskDescription is received by an Executor, the Executor needs to first get the
 * list of JARs and files and add these to the classpath, and set the properties, before
 * deserializing the Task object (serializedTask). This is why the Properties are included
 * in the TaskDescription, even though they're also in the serialized task.
 * (2) Because a TaskDescription is serialized and sent to an executor for each task, efficient
 * serialization (both in terms of serialization time and serialized buffer size) is
 * important. For this reason, we serialize TaskDescriptions ourselves with the
 * TaskDescription.encode and TaskDescription.decode methods.  This results in a smaller
 * serialized size because it avoids serializing unnecessary fields in the Map objects
 * (which can introduce significant overhead when the maps are small).
 */
private[spark] class TaskQueue(
                                val executorId: String,
                                val name: String,
                                val addedFiles: Map[String, Long],
                                val addedJars: Map[String, Long],
                                val addedArchives: Map[String, Long],
                                val properties: Properties,
                                val resources: immutable.Map[String, ResourceInformation],
                                val serializedTask: ByteBuffer,
                              ) {

  override def toString: String = s"TaskQueue($name)"
}

private[spark] object TaskQueue {
  private def serializeStringLongMap(map: Map[String, Long], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    map.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeLong(value)
    }
  }

  private def serializeResources(map: immutable.Map[String, ResourceInformation],
                                 dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    map.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeUTF(value.name)
      dataOut.writeInt(value.addresses.size)
      value.addresses.foreach(dataOut.writeUTF(_))
    }
  }

  def encode(taskQueue: TaskQueue): ByteBuffer = {
    val bytesOut = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(bytesOut)

    dataOut.writeUTF(taskQueue.executorId)
    dataOut.writeUTF(taskQueue.name)

    // Write files.
    serializeStringLongMap(taskQueue.addedFiles, dataOut)

    // Write jars.
    serializeStringLongMap(taskQueue.addedJars, dataOut)

    // Write archives.
    serializeStringLongMap(taskQueue.addedArchives, dataOut)

    // Write properties.
    dataOut.writeInt(taskQueue.properties.size())
    taskQueue.properties.asScala.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      // SPARK-19796 -- writeUTF doesn't work for long strings, which can happen for property values
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
    }

    // Write resources.
    serializeResources(taskQueue.resources, dataOut)

    dataOut.writeInt(taskQueue.serializedTask.remaining())
    dataOut.flush()
    Utils.writeByteBuffer(taskQueue.serializedTask, bytesOut)

    dataOut.close()
    bytesOut.close()
    bytesOut.toByteBuffer
  }

  private def deserializeStringLongMap(dataIn: DataInputStream): HashMap[String, Long] = {
    val map = new HashMap[String, Long]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      map(dataIn.readUTF()) = dataIn.readLong()
      i += 1
    }
    map
  }

  private def deserializeResources(dataIn: DataInputStream):
  immutable.Map[String, ResourceInformation] = {
    val map = new HashMap[String, ResourceInformation]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      val resType = dataIn.readUTF()
      val name = dataIn.readUTF()
      val numIdentifier = dataIn.readInt()
      val identifiers = new ArrayBuffer[String](numIdentifier)
      var j = 0
      while (j < numIdentifier) {
        identifiers += dataIn.readUTF()
        j += 1
      }
      map(resType) = new ResourceInformation(name, identifiers.toArray)
      i += 1
    }
    map.toMap
  }

  def decode(byteBuffer: ByteBuffer): TaskQueue = {
    val dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer))
    val executorId = dataIn.readUTF()
    val name = dataIn.readUTF()

    // Read files.
    val taskFiles = deserializeStringLongMap(dataIn)

    // Read jars.
    val taskJars = deserializeStringLongMap(dataIn)

    // Read archives.
    val taskArchives = deserializeStringLongMap(dataIn)

    // Read properties.
    val properties = new Properties()
    val numProperties = dataIn.readInt()
    for (i <- 0 until numProperties) {
      val key = dataIn.readUTF()
      val valueLength = dataIn.readInt()
      val valueBytes = new Array[Byte](valueLength)
      dataIn.readFully(valueBytes)
      properties.setProperty(key, new String(valueBytes, StandardCharsets.UTF_8))
    }

    // Read resources.
    val resources = deserializeResources(dataIn)

    // Create a sub-buffer for the serialized task into its own buffer (to be deserialized later).
    val serializedTaskSize = dataIn.readInt()

    val serializedTask = byteBuffer.slice()
    var serializedPartition = serializedTask.duplicate()

    serializedTask.limit(serializedTaskSize)
    serializedPartition.position(serializedTaskSize)
    serializedPartition = serializedPartition.slice()

    new TaskQueue(executorId, name, taskFiles, taskJars, taskArchives, properties, resources, serializedTask)
  }
}
