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
package org.apache.spark.status.api.v1

import java.io.OutputStream
import java.util.{List => JList}
import java.util.zip.ZipOutputStream
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}

import scala.util.control.NonFatal

import org.apache.spark.{JobExecutionStatus, SparkContext}
import org.apache.spark.ui.UIUtils

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AbstractApplicationResource extends BaseAppResource {

  @GET
  @Path("jobs")
  def jobsList(@QueryParam("status") statuses: JList[JobExecutionStatus]): Seq[JobData] = {
    withUI(_.store.jobsList(statuses))
  }

  @GET
  @Path("jobs/{jobId: \\d+}")
  def oneJob(@PathParam("jobId") jobId: Int): JobData = withUI { ui =>
    try {
      ui.store.job(jobId)
    } catch {
      case _: NoSuchElementException =>
        throw new NotFoundException("unknown job: " + jobId)
    }
  }

  @GET
  @Path("executors")
  def executorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(true))

  @GET
  @Path("executors/{executorId}/threads")
  def threadDump(@PathParam("executorId") execId: String): Array[ThreadStackTrace] = withUI { ui =>
    if (execId != SparkContext.DRIVER_IDENTIFIER && !execId.forall(Character.isDigit)) {
      throw new BadParameterException(
        s"Invalid executorId: neither '${SparkContext.DRIVER_IDENTIFIER}' nor number.")
    }

    val safeSparkContext = ui.sc.getOrElse {
      throw new ServiceUnavailable("Thread dumps not available through the history server.")
    }

    ui.store.asOption(ui.store.executorSummary(execId)) match {
      case Some(executorSummary) if executorSummary.isActive =>
          val safeThreadDump = safeSparkContext.getExecutorThreadDump(execId).getOrElse {
            throw new NotFoundException("No thread dump is available.")
          }
          safeThreadDump
      case Some(_) => throw new BadParameterException("Executor is not active.")
      case _ => throw new NotFoundException("Executor does not exist.")
    }
  }

  @GET
  @Path("allexecutors")
  def allExecutorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(false))

  @Path("stages")
  def stages(): Class[StagesResource] = classOf[StagesResource]

  @GET
  @Path("storage/rdd")
  def rddList(): Seq[RDDStorageInfo] = withUI(_.store.rddList())

  @GET
  @Path("storage/rdd/{rddId: \\d+}")
  def rddData(@PathParam("rddId") rddId: Int): RDDStorageInfo = withUI { ui =>
    try {
      ui.store.rdd(rddId)
    } catch {
      case _: NoSuchElementException =>
        throw new NotFoundException(s"no rdd found w/ id $rddId")
    }
  }

  @GET
  @Path("environment")
  def environmentInfo(): ApplicationEnvironmentInfo = withUI(_.store.environmentInfo())

  @GET
  @Path("logs")
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def getEventLogs(): Response = {
    // Retrieve the UI for the application just to do access permission checks. For backwards
    // compatibility, this code also tries with attemptId "1" if the UI without an attempt ID does
    // not exist.
    try {
      withUI { _ => }
    } catch {
      case _: NotFoundException if attemptId == null =>
        attemptId = "1"
        withUI { _ => }
        attemptId = null
    }

    try {
      val fileName = if (attemptId != null) {
        s"eventLogs-$appId-$attemptId.zip"
      } else {
        s"eventLogs-$appId.zip"
      }

      val stream = new StreamingOutput {
        override def write(output: OutputStream): Unit = {
          val zipStream = new ZipOutputStream(output)
          try {
            uiRoot.writeEventLogs(appId, Option(attemptId), zipStream)
          } finally {
            zipStream.close()
          }

        }
      }

      Response.ok(stream)
        .header("Content-Disposition", s"attachment; filename=$fileName")
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
        .build()
    } catch {
      case NonFatal(_) =>
        throw new ServiceUnavailable(s"Event logs are not available for app: $appId.")
    }
  }

  /**
   * This method needs to be last, otherwise it clashes with the paths for the above methods
   * and causes JAX-RS to not find things.
   */
  @Path("{attemptId}")
  def applicationAttempt(): Class[OneApplicationAttemptResource] = {
    if (attemptId != null) {
      throw new NotFoundException(httpRequest.getRequestURI())
    }
    classOf[OneApplicationAttemptResource]
  }

}

private[v1] class OneApplicationResource extends AbstractApplicationResource {

  @GET
  def getApp(): ApplicationInfo = {
    val app = uiRoot.getApplicationInfo(appId)
    app.getOrElse(throw new NotFoundException("unknown app: " + appId))
  }

  @GET
  @Path("frame")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def frame(): String = {
    val app = uiRoot.getApplicationInfo(appId)
    val jobs = withUI(_.store.jobsList(null))
    app.getOrElse(throw new NotFoundException("unknown app: " + appId))
//    s"Got an appdId $app.get.id and stages ${jobs.size}"
    val jobList = withUI(_.store.jobsList(null))
//    val jobMap = scala.collection.mutable.Map.empty[Int, JobData]
    val stageIdToJob = scala.collection.mutable.Map.empty[Int, JobData]
    for(job <- jobList) job.stageIds.foreach(stageId => stageIdToJob += stageId -> job)
    val stageList = withUI(_.store.stageList(null))
//    val stageMap = scala.collection.mutable.Map.empty[Int, StageData]
//    for(stage <- stageList) stageMap += stage.stageId -> withUI(_.store.stageWithDetails(stage))
    var resultCSV = "jobId,status,stageId,name,taskId,index,attempt,executorId,duration," +
      "sojournTime,waitingTime," +
      "taskLocality,executorDeserializeTime,executorRunTime,resultSize,jvmGcTime," +
      "resultSerializationTime,memoryBytesSpilled,diskBytesSpilled,peakExecutionMemory," +
      "bytesRead,recordsRead,readTime,locationExecId,readMethod,cachedBlock,bytesWritten," +
      "recordsWritten,shuffleRemoteBlocksFetched,shuffleLocalBlocksFetched,shuffleFetchWaitTime," +
      "remoteBytesRead,shuffleRemoteBytesReadToDisk,shuffleLocalBytesRead,shuffleRecordsRead," +
      "shuffleBytesWritten,shuffleWriteTime,shuffleRecordsWritten\n"
    for(stage_ <- stageList) {
      val stage = withUI(_.store.stageWithDetails(stage_))
      if (stage.tasks.nonEmpty) {
        val job = stageIdToJob(stage.stageId)
        val jobId = job.jobId
        // If completionTime non empty, submission time also have to be non empty
        val sojournTime = if (job.completionTime.nonEmpty) job.completionTime.get.getTime - job
          .submissionTime.get.getTime else -1
        for ((taskId, taskData) <- stage.tasks.get) {
          resultCSV += s"" +
            s"$jobId," +
            s"${stage.status}," +
            s"${stage.stageId}," +
            s"${stage.name}," +
            s"$taskId," +
            s"${taskData.index}," +
            s"${taskData.attempt}," +
            s"${taskData.executorId}," +
            s"${taskData.duration.getOrElse(-1)}," +
            s"$sojournTime," +
            // Could throw error
            s"${taskData.launchTime.getTime - job.submissionTime.get.getTime}," +
            s"${taskData.taskLocality}," +
            s"${taskData.taskMetrics.map(_.executorDeserializeTime).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.executorRunTime).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.resultSize).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.jvmGcTime).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.resultSerializationTime).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.memoryBytesSpilled).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.diskBytesSpilled).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.peakExecutionMemory).getOrElse(-1)}," +
            // Bytes read should be calculated from single read executions
            s"${taskData.taskMetrics.map(_.inputMetrics.bytesRead).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.inputMetrics.recordsRead).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.inputMetrics.readExecId.lastOption
              .map(_.readTime).getOrElse(-1)).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.inputMetrics.readExecId.lastOption
              .map(_.locationExecId).getOrElse("Nothing read")).getOrElse("Nothing read")}," +
            s"${taskData.taskMetrics.map(_.inputMetrics.readExecId.lastOption
              .map(_.readMethod).getOrElse("")).getOrElse("")}," +
            s"${taskData.taskMetrics.map(_.inputMetrics.readExecId.lastOption
              .map(_.cachedBlock).getOrElse("false")).getOrElse("false")}," +
            s"${taskData.taskMetrics.map(_.outputMetrics.bytesWritten).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.outputMetrics.recordsWritten).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.remoteBlocksFetched)
              .getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.localBlocksFetched).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.remoteBytesRead).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.remoteBytesReadToDisk)
              .getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.localBytesRead).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleReadMetrics.recordsRead).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleWriteMetrics.bytesWritten).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleWriteMetrics.writeTime).getOrElse(-1)}," +
            s"${taskData.taskMetrics.map(_.shuffleWriteMetrics.recordsWritten).getOrElse(-1)}," +
            s"\n"
          }
        }
    }
//    s"Got some mapped date ${jobMap.size}|${stageMap.size}|${stageMap.head.toString}"
    resultCSV
  }

}

private[v1] class OneApplicationAttemptResource extends AbstractApplicationResource {

  @GET
  def getAttempt(): ApplicationAttemptInfo = {
    uiRoot.getApplicationInfo(appId)
      .flatMap { app =>
        app.attempts.find(_.attemptId.contains(attemptId))
      }
      .getOrElse {
        throw new NotFoundException(s"unknown app $appId, attempt $attemptId")
      }
  }

}
