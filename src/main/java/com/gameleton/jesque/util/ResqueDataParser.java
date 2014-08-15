package com.gameleton.jesque.util;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.meta.QueueInfo;
import net.greghaines.jesque.meta.WorkerInfo;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Date;
import java.util.List;

/**
 * Created by levin on 7/23/2014.
 */
public class ResqueDataParser {

    public static JsonObject parseQueueInfo(QueueInfo queueInfo){
        JsonObject json = new JsonObject();
        json.putString("name", queueInfo.getName());
        json.putNumber("size", queueInfo.getSize());

        JsonArray jobs = new JsonArray();
        for (Job job : queueInfo.getJobs()) {
            jobs.add(parseJob(job));
        }
        json.putArray("jobs", jobs);
        return json;

    }

    public static JsonObject parseJob(Job job){
        JsonObject json = new JsonObject();
        json.putString("className", job.getClassName());
        return json;
    }

    public static JsonObject parseWorkerInfo(WorkerInfo workerInfo){

        JsonObject json = new JsonObject();
        json.putString("name", workerInfo.getName());
        json.putString("state", workerInfo.getState().name());
        json.putString("started", workerInfo.getStarted().toString());
        json.putNumber("processed", workerInfo.getProcessed());
        json.putNumber("failed", workerInfo.getFailed());
        json.putString("host", workerInfo.getHost());
        json.putString("pid", workerInfo.getPid());
        json.putObject("status", parseWorkerStatus(workerInfo.getStatus()));

        return json;

    }

    public static JsonObject parseWorkerStatus(WorkerStatus workerStatus){
        JsonObject json = new JsonObject();
        json.putString("runAt", workerStatus.getRunAt().toString());
        json.putString("queue", workerStatus.getQueue());
        json.putObject("payload", parseJob(workerStatus.getPayload()));

        return json;
    }

    public static JsonObject parseJobFailure(JobFailure jobFailure){

        JsonObject json = new JsonObject();
        json.putString("worker", jobFailure.getWorker());
        json.putString("queue", jobFailure.getQueue());
        json.putObject("payload", parseJob(jobFailure.getPayload()));
        json.putString("exception", jobFailure.getExceptionString());
        json.putString("error", jobFailure.getError());
        json.putString("failedAt", jobFailure.getFailedAt().toString());
        json.putString("retriedAt", jobFailure.getRetriedAt().toString());


        return json;
    }




}
