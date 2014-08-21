package com.gameleton.jesque.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gameleton.jesque.deplayed.JesqueDelayedJobRunner;
import com.gameleton.jesque.deplayed.JesqueDelayedJobService;
import com.gameleton.jesque.scheduled.JesqueScheduleRunner;
import com.gameleton.jesque.scheduled.JesqueScheduleService;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.admin.Admin;
import net.greghaines.jesque.admin.AdminClient;
import net.greghaines.jesque.admin.AdminClientImpl;
import net.greghaines.jesque.admin.AdminImpl;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.meta.WorkerInfo;
import net.greghaines.jesque.meta.dao.WorkerInfoDAO;
import net.greghaines.jesque.meta.dao.impl.WorkerInfoDAORedisImpl;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.worker.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by levin on 8/14/2014.
 */
public class JesqueServiceImpl implements com.gameleton.jesque.JesqueService {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueServiceImpl.class);

    private final Vertx vertx;

    public Vertx vertx() {
      return vertx;
    }

    private final Config jesqueConfig;

    public Config jesqueConfig() {
      return jesqueConfig;
    }

    private final Pool<Jedis> pool;

    public Pool<Jedis> pool() {
      return pool;
    }

    private final JesqueScheduleService jesqueScheduleService;
    //private final JesqueDelayedJobService jesqueDelayedJobService;

    //private final JesqueDelayedJobRunner jesqueDelayedJobRunner;
    private final JesqueScheduleRunner jesqueScheduleRunner;

    private Client jesqueClient;
    private WorkerInfoDAO workerInfoDao;
    private List<Worker> workers = Collections.synchronizedList(new ArrayList<Worker>());
    private AdminClient jesqueAdminClient;

    public JesqueServiceImpl(Vertx vertx, JsonObject config){
        this.vertx = vertx;

        ConfigBuilder builder = new ConfigBuilder();

        JsonObject jesqueCfg = config.getObject("jesque");

        builder.withHost(jesqueCfg.getString("redis_host"))
                .withPort(jesqueCfg.getInteger("redis_port"))
                .withNamespace(jesqueCfg.getString("redis_namespace"));

        this.jesqueConfig = builder.build();

        JedisPoolConfig poolConfig = new JedisPoolConfig();

        this.pool = PoolUtils.createJedisPool(jesqueConfig, poolConfig);

        this.workerInfoDao = new WorkerInfoDAORedisImpl(jesqueConfig, pool);

        this.jesqueClient = new ClientImpl(jesqueConfig);

        this.jesqueAdminClient = new AdminClientImpl(jesqueConfig);

        //this.jesqueDelayedJobService = new JesqueDelayedJobService(this);

        //this.jesqueDelayedJobRunner = new JesqueDelayedJobRunner(jesqueDelayedJobService, vertx, 20000);

        this.jesqueScheduleService = new JesqueScheduleService(this);

        this.jesqueScheduleRunner = new JesqueScheduleRunner(jesqueScheduleService, vertx);

        //jesqueDelayedJobRunner.start();

        jesqueScheduleRunner.start();

        JsonArray workersCfg = config.getArray("workers");

        JsonArray jobsCfg = config.getArray("jobs");

        startWorkersFromConfig(workersCfg);

        startJobsFromConfig(jobsCfg);
    }

    @Override
    public void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, Object... args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, jesqueJobQueue, jesqueJobName, Arrays.asList(args));
    }

    @Override
    public void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, List args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, DateTimeZone.getDefault(), jesqueJobQueue, jesqueJobName, args);
    }

    @Override
    public void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, Object... args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, timeZone, jesqueJobQueue, jesqueJobName, Arrays.asList(args));
    }

    @Override
    public void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, List args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, timeZone, jesqueJobQueue, jesqueJobName, args);
    }

    @Override
    public void enqueue(String queueName, Job job) {
        jesqueClient.enqueue(queueName, job);
    }

    @Override
    public void enqueue(String queueName, String jobName, List args) {
        enqueue(queueName, new Job(jobName, args));
    }

    @Override
    public void enqueue(String queueName, Class jobClazz, List args) {
        enqueue(queueName, jobClazz.getSimpleName(), args);
    }

    @Override
    public void enqueue(String queueName, String jobName, Object... args) {
        enqueue(queueName, new Job(jobName, args));
    }

    @Override
    public void enqueue(String queueName, Class jobClazz, Object... args) {
        enqueue(queueName, jobClazz.getSimpleName(), args);
    }

    @Override
    public void priorityEnqueue(String queueName, Job job) {
        jesqueClient.priorityEnqueue(queueName, job);
    }

    @Override
    public void priorityEnqueue(String queueName, String jobName, Object... args) {
        priorityEnqueue(queueName, new Job(jobName, args));
    }

    @Override
    public void priorityEnqueue(String queueName, Class jobClazz, Object... args) {
        priorityEnqueue(queueName, jobClazz.getSimpleName(), args);
    }

//    @Override
//    public void enqueueAt(DateTime dateTime, String queueName, Job job) {
//        try {
//            jesqueDelayedJobService.enqueueAt(dateTime, queueName, job);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void enqueueAt(DateTime dateTime, String queueName, String jobName, Object... args) {
//        enqueueAt( dateTime, queueName, new Job(jobName, args) );
//    }
//
//    @Override
//    public void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, Object... args) {
//        enqueueAt( dateTime, queueName, jobClazz.getSimpleName(), args);
//    }
//
//    @Override
//    public void enqueueAt(DateTime dateTime, String queueName, String jobName, List args) {
//        enqueueAt( dateTime, queueName, new Job(jobName, args) );
//    }
//
//    @Override
//    public void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, List args) {
//        enqueueAt( dateTime, queueName, jobClazz.getSimpleName(), args );
//    }


//    @Override
//    public void enqueueIn(Integer millisecondDelay, String queueName, Job job) {
//        enqueueAt( new DateTime().plusMillis(millisecondDelay), queueName, job );
//    }
//
//    @Override
//    public void enqueueIn(Integer millisecondDelay, String queueName, String jobName, Object... args) {
//        enqueueIn( millisecondDelay, queueName, new Job(jobName, args) );
//    }
//
//    @Override
//    public void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, Object... args) {
//        enqueueIn( millisecondDelay, queueName, jobClazz.getSimpleName(), args );
//    }
//
//    @Override
//    public void enqueueIn(Integer millisecondDelay, String queueName, String jobName, List args) {
//        enqueueIn( millisecondDelay, queueName, new Job(jobName, args) );
//    }
//
//    @Override
//    public void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, List args) {
//        enqueueIn( millisecondDelay, queueName, jobClazz.getSimpleName(), args );
//    }

    @Override
    public Worker startWorker(String queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put(jobName, jobClass);
        return startWorker(Arrays.asList(queueName), jobTypes, exceptionHandler, paused);
    }

    @Override
    public Worker startWorker(List queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put(jobName, jobClass);
        return startWorker(queueName, jobTypes, exceptionHandler, paused);
    }

    @Override
    public Worker startWorker(String queueName, Map<String, Class<?>> jobTypes, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        return startWorker(Arrays.asList(queueName), jobTypes, exceptionHandler, paused);
    }

    @Override
    public Worker startWorker(List<String> queues, Map<String, Class<?>> jobTypes, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        JobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Worker worker = new WorkerImpl(jesqueConfig, queues, jobFactory);

        if (exceptionHandler != null)
            worker.setExceptionHandler(exceptionHandler);

        if (paused) {
            worker.togglePause(paused);
        }

        workers.add(worker);

        // create an Admin for this worker (makes it possible to administer across a cluster)
//        Admin admin = new AdminImpl(jesqueConfig);
//        admin.setWorker(worker);

        //        vertx.runOnContext(new Handler<Void>() {
//            @Override
//            public void handle(Void event) {
//                admin.run();
//            }
//        });

        //add listener for removing workers list when it stopped
        WorkerLifecycleListener workerLifeCycleListener = new WorkerLifecycleListener(this);
        worker.getWorkerEventEmitter().addListener(workerLifeCycleListener, WorkerEvent.WORKER_STOP);

        vertx.runOnContext(new Handler<Void>() {
            @Override
            public void handle(Void event) {
                worker.run();
            }
        });



        return worker;
    }

    @Override
    public void stop() {
        LOG.info("Stopping ${workers.size()} jesque workers");

        //jesqueDelayedJobRunner.stop();

        jesqueScheduleRunner.stop();

        workers.forEach(worker -> {
            try{
                LOG.debug("Stopping worker processing queues: " + worker.getQueues());
                worker.end(true);
                //worker.join(5000);
            } catch(Exception ex) {
                LOG.error("Exception ending jesque worker", ex);
            }
        });
    }

    void startJobsFromConfig(JsonArray jobsCfg) {
        JsonObject jobCfg;
        String jobQueue, jobName;
        List args;
        for (int i = 0; i < jobsCfg.size(); i++) {
            jobCfg = jobsCfg.get(i);
            jobQueue = jobCfg.getString("jobQueue");
            jobName = jobCfg.getString("jobName");
            args = jobCfg.getArray("args", new JsonArray()).toList();
            if(jobCfg.containsField("cronName") && jobCfg.containsField("cronExpression")){
                String cronName = jobCfg.getString("cronName");
                String cronExpression = jobCfg.getString("cronExpression");
                schedule(cronName, cronExpression, jobQueue, jobName, args);
            }
            else if(jobCfg.containsField("deplay")){
//                int delay = jobCfg.getInteger("deplay");
//                enqueueIn(delay, jobQueue, jobName, args);
                LOG.info("deplay job not implemented");
            }
            else{
                enqueue(jobQueue, jobName, args);
            }
        }
    }

    void startWorkersFromConfig(JsonArray workersCfg) {
        JsonObject workerCfg;
        List queueNames;
        Map<String, Class<?>> jobTypes = new HashMap<>();
        for (int i = 0; i < workersCfg.size(); i++) {
            workerCfg = workersCfg.get(i);
            queueNames = workerCfg.getArray("queueNames").toList();
            Map<String, String> jobTypesCfg = (Map)workerCfg.getObject("jobTypes").toMap();
            for (Map.Entry<String, String> entry : jobTypesCfg.entrySet()) {
                try {
                    jobTypes.put(entry.getKey(), Class.forName(entry.getValue()));
                }
                catch (Exception ex){
                    LOG.error("Failed to load class of " + entry.getValue());
                }
            }
            startWorker(queueNames, jobTypes, null, workerCfg.getBoolean("startPaused", false));
        }
    }

    @Override
    public void pruneWorkers() {
        try {
            final String hostName = InetAddress.getLocalHost().getHostName();
            List<WorkerInfo> workerInfos = workerInfoDao.getAllWorkers();
            workerInfos.forEach(workerInfo -> {
                if(hostName.equals(workerInfo.getHost())){
                    LOG.debug("Removing stale worker $workerInfo.name");
                    workerInfoDao.removeWorker(workerInfo.getName());
                }
            });

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeWorkerFromLifecycleTracking(Worker worker) {
        LOG.debug("Removing worker " + worker.getName() + " from lifecycle tracking");
        workers.remove(worker);
    }

    @Override
    public void pauseAllWorkersOnThisNode() {
        LOG.info("Pausing all " + workers.size() + " jesque workers on this node");

        workers.forEach(worker -> {
            LOG.debug("Pausing worker processing queues: ${worker.queues}");
            worker.togglePause(true);
        });
    }

    @Override
    public void resumeAllWorkersOnThisNode() {
        LOG.info("Resuming all " + workers.size() + " jesque workers on this node");

        workers.forEach(worker -> {
            LOG.debug("Pausing worker processing queues: ${worker.queues}");
            worker.togglePause(true);
        });
    }

    @Override
    public void pauseAllWorkersInCluster() {
        LOG.debug("Pausing all workers in the cluster");
        jesqueAdminClient.togglePausedWorkers(true);
    }

    @Override
    public void resumeAllWorkersInCluster() {
        LOG.debug("Resuming all workers in the cluster");
        jesqueAdminClient.togglePausedWorkers(false);
    }

    @Override
    public void shutdownAllWorkersInCluster() {
        LOG.debug("Shutting down all workers in the cluster");
        jesqueAdminClient.shutdownWorkers(true);
    }

    @Override
    public boolean areAllWorkersInClusterPaused() {
        return workerInfoDao.getActiveWorkerCount() == 0;
    }
}
