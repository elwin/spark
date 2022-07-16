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

package org.apache.spark

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import collection.JavaConverters._

object Time {
  case class Key(Name: String, ThreadID: Long)

  case class Entry(start: Long, count: Int, duration: Long)

  private val durations = new ConcurrentHashMap[Key, Entry]()
  private val taskGap = new ConcurrentHashMap[Long, Long]()

  def time[R](block: => R, name: String): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    val duration = t1 - t0
    val threadID = Thread.currentThread().getId

    durations.compute(Key(Name = name, ThreadID = threadID), (_, value) =>
      if (value == null) Entry(start = System.nanoTime(), count = 1, duration = duration)
      else Entry(start = value.start, count = value.count + 1, duration = value.duration + duration)
    )

    result
  }

  def launchedTask(taskId: Long): Unit = {
    taskGap.put(taskId, System.nanoTime())
  }

  def finishedTask(taskId: Long): Unit = {
    val duration = System.nanoTime() - taskGap.remove(taskId)

    val threadID = Thread.currentThread().getId
    durations.compute(Key(Name = "gap_duration", ThreadID = threadID), (_, value) =>
      if (value == null) Entry(start = System.nanoTime(), count = 1, duration = duration)
      else Entry(start = value.start, count = value.count + 1, duration = value.duration + duration)
    )
  }

  def printProfilingInformation(): mutable.ArrayBuffer[String] = {
    val durationsList = mutable.ArrayBuffer[String]()

    for (key <- durations.keySet().asScala) {
      val curTime = System.nanoTime()

      val entry = durations.put(key, Entry(start = curTime, count = 0, duration = 0))

      val bucketSize = curTime - entry.start
      val average = if (entry.count > 0) entry.duration / entry.count else 0
      val fraction = entry.duration.toDouble / bucketSize.toDouble

      durationsList.append(
        s"""elw4: {"type": "profiling", "name": "${
          key.Name
        }", "total": ${
          entry.duration
        }, "count": ${
          entry.count
        }, "average": $average, "fraction": $fraction, "bucket_size": $bucketSize, "thread_id": ${
          key.ThreadID
        }, "timestamp": $curTime}""")
    }

    durationsList
  }
}