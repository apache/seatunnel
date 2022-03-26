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
  * spark指标监听
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
  private val tmpRecordsRead = new LongAccumulator
  private val tmpRecordsWritten = new LongAccumulator
  private val readBytes = new LongAccumulator
  private val recordsWritten = new LongAccumulator
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
    * 获取阶段性指标值
    *
    * @param stageCompleted 阶段完成
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
    tmpRecordsRead.add(inputMetrics._recordsRead.value)
    tmpRecordsWritten.add(outputMetrics._recordsWritten.value)
  }

  /**
    * 获取每个task指标值,这里需要注意的是分布式系统每个task是运行在不同的执行器上的,
    * 本地运行在driver上无需二次累加值,因此部分shuffle值需要做累加
    *
    * @param taskEnd taskEnd
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    tasksCompleted.incrementAndGet()
    //内存使用峰值
    peakExecutionMemory.add(taskEnd.taskMetrics.peakExecutionMemory)
    //只保留一个值
    taskEndReason = taskEnd.reason.toString
    jvmGCTime.add(taskEnd.taskMetrics.jvmGCTime / unit)
    executorCpuTime = new java.math.BigDecimal(taskEnd.taskMetrics.executorCpuTime.doubleValue() / unit)
      .setScale(2, RoundingMode.UP).toPlainString
    executorRunTime = new java.math.BigDecimal(taskEnd.taskMetrics.executorRunTime.doubleValue() / 1000)
      .setScale(2, RoundingMode.UP).toPlainString
    val shuffleReadMetrics = taskEnd.taskMetrics.shuffleReadMetrics
    val shuffleWriteMetrics = taskEnd.taskMetrics.shuffleWriteMetrics
    recordsRead.add(shuffleReadMetrics._recordsRead.value)
    readBytes.add(shuffleReadMetrics.totalBytesRead)
    recordsWritten.add(shuffleWriteMetrics._recordsWritten.value)
    writeBytes.add(shuffleWriteMetrics._bytesWritten.value)
  }

  /**
    * 获取job个数以及job结束后的执行时间
    *
    * @param jobEnd jobEnd
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobsCompleted.incrementAndGet()
    jobRuntime = (jobEnd.time - startTime)/1000
  }

  /**
    * 应用程序结束,后指标收集输出或者输出到外部系统中,生成任务执行报告
    *
    * @param applicationEnd 应用程序结束
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    //处理无shuffle阶段值统计
    if (recordsRead.sum == 0){
      recordsRead.add(tmpRecordsRead.value)
    }
    if (recordsWritten.sum == 0){
      recordsWritten.add(tmpRecordsWritten.value)
    }
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
    metrics.put("recordsWritten", recordsWritten.value.asInstanceOf[AnyRef])
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
