package com.gameleton.jesque;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.ExceptionHandler;
import net.greghaines.jesque.worker.Worker;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by levin on 8/15/2014.
 */
public interface JesqueService {

    Vertx vertx();

    Pool<Jedis> pool();

    Config jesqueConfig();

    void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, Object... args);

    void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, List args);

    void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, Object... args);

    void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, List args);

    void enqueue(String queueName, Job job);

    void enqueue(String queueName, String jobName, List args);

    void enqueue(String queueName, Class jobClazz, List args);

    void enqueue(String queueName, String jobName, Object... args);

    void enqueue(String queueName, Class jobClazz, Object... args);

    void priorityEnqueue(String queueName, Job job);

    void priorityEnqueue(String queueName, String jobName, Object... args);

    void priorityEnqueue(String queueName, Class jobClazz, Object... args);

//    void enqueueAt(DateTime dateTime, String queueName, Job job);
//
//    void enqueueAt(DateTime dateTime, String queueName, String jobName, Object... args);
//
//    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, Object... args);
//
//    void enqueueAt(DateTime dateTime, String queueName, String jobName, List args);
//
//    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, List args);

//    void enqueueIn(Integer millisecondDelay, String queueName, Job job);
//
//    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, Object... args);
//
//    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, Object... args);
//
//    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, List args);
//
//    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, List args);

    Worker startWorker(String queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler,
                       boolean paused);

    Worker startWorker(List queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler,
                       boolean paused);

    Worker startWorker(String queueName, Map<String, Class<?>> jobTypes, ExceptionHandler exceptionHandler,
                       boolean paused);

    Worker startWorker(List<String> queues, Map<String, Class<?>> jobTypes, ExceptionHandler exceptionHandler,
                       boolean paused);

    void stop();

    void pruneWorkers();

    void removeWorkerFromLifecycleTracking(Worker worker);

    void pauseAllWorkersOnThisNode();

    void resumeAllWorkersOnThisNode();

    void pauseAllWorkersInCluster();

    void resumeAllWorkersInCluster();

    void shutdownAllWorkersInCluster();

    boolean areAllWorkersInClusterPaused();
}
