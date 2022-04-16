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


package org.apache.spark.executor.seatunnel.metric

import java.text.SimpleDateFormat
import org.apache.seatunnel.spark.utils.PrintUtil
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.math.RoundingMode
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

/**
  * spark metric
  *
  * */

class SparkMetricsListener(sparkSession: SparkSession) extends SparkListener {
  private final val logger: Logger = LoggerFactory.getLogger(classOf[SparkMetricsListener])
  private final val unit: Long = 100000000
  private val jobsCompleted = new AtomicInteger(0)
  private val stagesCompleted = new AtomicInteger(0)
  private val tasksCompleted = new AtomicInteger(0)
  private val recordsRead = new LongAccumulator
  private val stageTotalInputBytes = new LongAccumulator
  private val stageTotalOutputBytes = new LongAccumulator
  private val readBytes = new LongAccumulator
  private var recordsWritten = 0L
  private val writeBytes = new LongAccumulator
  private val jvmGCTime = new LongAccumulator
  private var executorCpuTime: String = "0"
  private var executorRunTime: String = "0"
  private var jobRuntime: Double = 0
  private val peakExecutionMemory = new LongAccumulator
  private val numTasks = new LongAccumulator
  private var submissionTime: Double = 0
  private var completionTime: Double = 0
  private val attemptNumber = new LongAccumulator
  private var taskEndReason: String = ""
  private val failureReason: java.util.Map[String, String] = new java.util.HashMap[String, String]()
  private val metrics: java.util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
  private val jobName: String = sparkSession.sparkContext.appName
  private val startTime = sparkSession.sparkContext.startTime
  private val applicationId: String = sparkSession.sparkContext.applicationId
  private val maxNumConcurrentTasks: String = String.valueOf(sparkSession.sparkContext.maxNumConcurrentTasks())

  /**
    * Get stage index value
    *
    * @param stageCompleted stageCompleted
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stagesCompleted.incrementAndGet()
    if (stageCompleted.stageInfo.failureReason.nonEmpty) {
      failureReason.put(stageCompleted.stageInfo.name, stageCompleted.stageInfo.failureReason.get)
    }
    numTasks.add(stageCompleted.stageInfo.numTasks)
    submissionTime = (stageCompleted.stageInfo.submissionTime.get / unit) / 10000
    completionTime = (stageCompleted.stageInfo.completionTime.get / unit) / 10000
    attemptNumber.add(stageCompleted.stageInfo.attemptNumber())
    val inputMetrics = stageCompleted.stageInfo.taskMetrics.inputMetrics
    val outputMetrics = stageCompleted.stageInfo.taskMetrics.outputMetrics
    stageTotalInputBytes.add(inputMetrics._bytesRead.value)
    stageTotalOutputBytes.add(outputMetrics._bytesWritten.value)
    recordsWritten = outputMetrics.recordsWritten
    writeBytes.add(outputMetrics._bytesWritten.value)
  }

  /**
    * To get the index value of each task,
    * it should be noted here that each task of the distributed system runs on a different executor,
    * and the local running on the driver does not need to accumulate the value twice,
    * so some shuffle values need to be accumulated
    *
    *
    * @param taskEnd taskEnd
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    tasksCompleted.incrementAndGet()
    //peak memory usage
    peakExecutionMemory.add(taskEnd.taskMetrics.peakExecutionMemory)
    //keep only one value
    taskEndReason = taskEnd.reason.toString
    jvmGCTime.add(taskEnd.taskMetrics.jvmGCTime / unit)
    executorCpuTime = new java.math.BigDecimal(taskEnd.taskMetrics.executorCpuTime.doubleValue() / unit)
      .setScale(2, RoundingMode.UP).toPlainString
    executorRunTime = new java.math.BigDecimal(taskEnd.taskMetrics.executorRunTime.doubleValue() / 1000)
      .setScale(2, RoundingMode.UP).toPlainString
    val shuffleReadMetrics = taskEnd.taskMetrics.shuffleReadMetrics
    val shuffleWriteMetrics = taskEnd.taskMetrics.shuffleWriteMetrics
    recordsRead.add(taskEnd.taskMetrics.inputMetrics._recordsRead.value)
    recordsRead.add(shuffleReadMetrics._recordsRead.value)
    readBytes.add(shuffleReadMetrics.totalBytesRead)
  }

  /**
    * Get the number of jobs and the execution time after the job ends
    *
    * @param jobEnd jobEnd
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobsCompleted.incrementAndGet()
    jobRuntime = (jobEnd.time - startTime)/1000
  }

  /**
    * After the application ends, the indicators are collected and output or output to an external system
   * to generate a task execution report
    *
    * @param applicationEnd applicationEnd
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    //Handling no shuffle stage value statistics
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    metrics.put("jobName", jobName)
    metrics.put("createTime", dateFormat.format(new Date(startTime)))
    metrics.put("applicationId", applicationId)
    metrics.put("maxNumConcurrentTasks", maxNumConcurrentTasks)
    metrics.put("jobsCompleted", jobsCompleted.get().asInstanceOf[AnyRef])
    metrics.put("jobRuntime", jobRuntime.asInstanceOf[AnyRef])
    metrics.put("stagesCompleted", stagesCompleted.get().asInstanceOf[AnyRef])
    metrics.put("tasksCompleted", tasksCompleted.asInstanceOf[AnyRef])
    metrics.put("stageSubmissionTime", submissionTime.asInstanceOf[AnyRef])
    metrics.put("stageCompletionTime", completionTime.asInstanceOf[AnyRef])
    metrics.put("stageAttemptNumber", attemptNumber.value.asInstanceOf[AnyRef])
    metrics.put("executorRuntime", executorRunTime.asInstanceOf[AnyRef])
    metrics.put("recordsRead", recordsRead.value.asInstanceOf[AnyRef])
    metrics.put("shuffleReadBytes", readBytes.value.asInstanceOf[AnyRef])
    metrics.put("recordsWritten", recordsWritten.asInstanceOf[AnyRef])
    metrics.put("shuffleWriteBytes", writeBytes.value.asInstanceOf[AnyRef])
    metrics.put("totalInputBytes", stageTotalInputBytes.value.asInstanceOf[AnyRef])
    metrics.put("totalOutputBytes", stageTotalOutputBytes.value.asInstanceOf[AnyRef])
    metrics.put("jvmGCTime", jvmGCTime.value.asInstanceOf[AnyRef])
    metrics.put("executorCpuTime", executorCpuTime.asInstanceOf[AnyRef])
    metrics.put("peakExecutionMemory", peakExecutionMemory.value.asInstanceOf[AnyRef])
    metrics.put("taskEndReason", taskEndReason.mkString)
    metrics.put("failureReason", failureReason.toString)
    PrintUtil.printResult(metrics)
  }
}
