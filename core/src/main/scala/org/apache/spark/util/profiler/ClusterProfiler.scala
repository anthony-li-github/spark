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

package org.apache.spark.util.profiler

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Semaphore

import scala.collection.JavaConverters._

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.util.profiler.Reporter._
import org.apache.spark.util.profiler.reports.InfluxDbReporter

private[spark] object ClusterProfiler {

  private val eventsQueue = new ConcurrentLinkedDeque[(Map[String, String], Event)]()
  private val eventSignal = new Semaphore(0)
  private val flushEvery = 100
  private var stopSignal = false

  private lazy val reporter = new InfluxDbReporter()

  private var appId: String = null
  private var hostName: String = null
  private var executorId: String = null
  private def defaultContext = Map(
    "appId" -> appId,
    "hostName" -> hostName,
    "executorId" -> executorId
  )

  val flushThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (!stopSignal) {
        eventSignal.acquire(flushEvery)
        val events = eventsQueue.iterator().asScala.toSeq
        eventsQueue.removeAll(events.asJava)
        reporter.report(events)
      }
    }
  })
  flushThread.start

  def startProfiling(appId: String, hostName: String, executorId: String): Unit = {
    this.appId = appId
    this.hostName = hostName
    this.executorId = executorId

    reportEvent(defaultContext, START_APP)
  }

  def stopProfiling(appId: String, hostName: String, executorId: String): Unit = {
    stopSignal = true
    reportEvent(defaultContext, END_APP)
    eventSignal.release(eventsQueue.size)
    flushThread.join()
  }

  def time[Res](func: () => Res, context: Map[String, String]): Res = {
    val contextBeforeFunction = defaultContext ++ context
    reportEvent(contextBeforeFunction, START)
    val startTime = System.currentTimeMillis()
    val response = func()
    val duration = System.currentTimeMillis() - startTime
    val contextAfterFunction = contextBeforeFunction ++ Map("duration" -> duration.toString)
    val finalContext = response match {
      case _res: ManagedBuffer =>
        contextAfterFunction + ("size" -> _res.size().toString)
      case _ => contextAfterFunction
    }
    reportEvent(finalContext, SUCCESS)
    response
  }

  def reportEvent(context: Map[String, String], event: Event = DEFAULT): Unit = {
    eventsQueue.push((
      defaultContext ++ context ++ Map("systemTimeMs" -> System.currentTimeMillis().toString),
      event
    ))
    eventSignal.release()
  }
}
