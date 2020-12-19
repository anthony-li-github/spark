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

package org.apache.spark.util.profiler.reports

import java.time.Instant

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.util.profiler.Reporter
import org.apache.spark.util.profiler.Reporter._


private[spark] class InfluxDbReporter extends Reporter {

  final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  lazy private val server = Option(System.getenv("InfluxDbHost")).getOrElse("http://localhost")
  lazy private val port = Option(System.getenv("InfluxDbPort")).getOrElse("8086")
  lazy private val token = Option(System.getenv("InfluxDbToken"))
  lazy private val org = Option(System.getenv("InfluxDbOrganization")).getOrElse("organization")
  lazy private val bucket = Option(System.getenv("InfluxDbBucket")).getOrElse("bucket")
  lazy private val client = createClient(server, port.toInt)

  protected def createClient(server: String, port: Int): Option[InfluxDBClient] = {
    val url = s"$server:$port"
    logger.info(String.format("Connecting to influxDB at %s", url))
    token match {
      case (Some(_token)) =>
        Some(InfluxDBClientFactory.create(url, _token.toCharArray, org, bucket))
      case _ => None
    }
  }

  def report(context: Map[String, String], event: Event): Unit = Future {
    client match {
      case Some(_client) =>
        val writeApi = _client.getWriteApi
        val point = constructPoint(context, event)
        try {
          logger.info(s"Sending line to influx db: ${point.toLineProtocol}")
          writeApi.writePoint(bucket, org, point)
        }
        catch {
          case error: Throwable =>
            logger.error(s"Failed to report to influx db: ${error.getMessage}")
            error.printStackTrace()
        }
      case None =>
    }

  }

  private def constructPoint(context: Map[String, String], event: Event): Point = {
    val functionName = context.getOrElse("function", "Unknown Function").toString
    val memoryUsage = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
    Point
      .measurement(s"${functionName}_$event")
      .addTags(context.asJava)
      .time(Instant.now(), WritePrecision.NS)
      .addField("memoryUsage", memoryUsage)
  }
}
