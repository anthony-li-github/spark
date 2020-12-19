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

import org.apache.spark.util.profiler.Reporter.Event

private[spark] abstract class Reporter {
  def report(context: Map[String, String], event: Event): Unit
}
private[spark] object Reporter {
  type Event = String
  final val START = "START"
  final val END = "END"
  final val START_APP = "START_APP"
  final val END_APP = "END_APP"
  final val DEFAULT = "NONE"
}
