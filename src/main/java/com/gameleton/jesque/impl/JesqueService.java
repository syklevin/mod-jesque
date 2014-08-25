package com.gameleton.jesque.impl;

import com.gameleton.jesque.scheduled.JesqueScheduleRunner;
import com.gameleton.jesque.scheduled.JesqueScheduleService;
import com.google.inject.Inject;
import com.google.inject.Injector;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.admin.AdminClient;
import net.greghaines.jesque.admin.AdminClientImpl;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.meta.WorkerInfo;
import net.greghaines.jesque.meta.dao.WorkerInfoDAO;
import net.greghaines.jesque.meta.dao.impl.WorkerInfoDAORedisImpl;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.worker.*;
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
public class JesqueService {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueService.class);

    private final Vertx vertx;
    private final JsonObject config;
    private final Injector injector;
    private final String instanceId;
    private Config jesqueConfig;
    private Pool<Jedis> pool;
    private JesqueScheduleService jesqueScheduleService;
    private Client jesqueClient;


    private List<Worker> workers = Collections.synchronizedList(new ArrayList<Worker>());

    public Vertx vertx() {
      return vertx;
    }

    public Config jesqueConfig() {
      return jesqueConfig;
    }

    public Pool<Jedis> pool() {
      return pool;
    }

    public String instanceId() {
      return instanceId;
    }

    public List<Worker> workers() {
      return workers;
    }

    @Inject
    public JesqueService(Vertx vertx, Injector injector){
        this.vertx = vertx;
        this.injector = injector;
        this.config = new JsonObject();
        this.instanceId = UUID.randomUUID().toString();
    }

    public void configure(JsonObject config){
        this.config.mergeIn(config);
    }

    public void start(){

        ConfigBuilder builder = new ConfigBuilder();
        JsonObject jesqueCfg = config.getObject("jesque");
        builder.withHost(jesqueCfg.getString("redis_host"))
                .withPort(jesqueCfg.getInteger("redis_port"))
                .withNamespace(jesqueCfg.getString("redis_namespace"));
        this.jesqueConfig = builder.build();
        this.pool = PoolUtils.createJedisPool(jesqueConfig, new JedisPoolConfig());
        this.jesqueClient = new ClientImpl(jesqueConfig);

        this.jesqueScheduleService = new JesqueScheduleService(this);

        JsonArray workersCfg = config.getArray("workers");
        startWorkersFromConfig(workersCfg);
        JsonArray jobsCfg = config.getArray("jobs");
        startJobsFromConfig(jobsCfg);

        this.jesqueScheduleService.start();

    }

    public void stop() {
        LOG.info("Stopping ${workers.size()} jesque workers");
        jesqueScheduleService.stop();

        workers.forEach(worker -> {
            try{
                LOG.debug("Stopping worker processing queues: " + worker.getQueues());
                worker.end(true);
//                worker.join(1000);
            } catch(Exception ex) {
                LOG.error("Exception ending jesque worker", ex);
            }
        });
        workers.clear();

        pool.destroy();
    }

    public void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, List args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, DateTimeZone.getDefault(), jesqueJobQueue, jesqueJobName, args);
    }

    public void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, Object... args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, timeZone, jesqueJobQueue, jesqueJobName, Arrays.asList(args));
    }

    public void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, List args) {
        jesqueScheduleService.schedule(jobName, cronExpressionString, timeZone, jesqueJobQueue, jesqueJobName, args);
    }

    public void enqueue(String queueName, Job job) {
        jesqueClient.enqueue(queueName, job);
    }

    public void enqueue(String queueName, String jobName, List args) {
        enqueue(queueName, new Job(jobName, args));
    }

    public void enqueue(String queueName, Class jobClazz, List args) {
        enqueue(queueName, jobClazz.getSimpleName(), args);
    }

    public void priorityEnqueue(String queueName, Job job) {
        jesqueClient.priorityEnqueue(queueName, job);
    }

    public void priorityEnqueue(String queueName, String jobName, List args) {
        priorityEnqueue(queueName, new Job(jobName, args));
    }

    public void priorityEnqueue(String queueName, Class jobClazz, List args) {
        priorityEnqueue(queueName, jobClazz.getSimpleName(), args);
    }

    public void delayEnqueue(long millisecondDelay, String queueName, Job job) {
        jesqueClient.delayedEnqueue(queueName, job, millisecondDelay);
    }

    public void delayEnqueue(long millisecondDelay, String queueName, String jobName, List args) {
        delayEnqueue(millisecondDelay, queueName, new Job(jobName, args));
    }

    public void delay(long millisecondDelay, String queueName, Class jobClazz, List args) {
        delayEnqueue(millisecondDelay, queueName, jobClazz.getSimpleName(), args);
    }


    public Worker startWorker(String queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put(jobName, jobClass);
        return startWorker(Arrays.asList(queueName), jobTypes, exceptionHandler, paused);
    }

    public Worker startWorker(List queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put(jobName, jobClass);
        return startWorker(queueName, jobTypes, exceptionHandler, paused);
    }

    public Worker startWorker(String queueName, Map<String, Class<?>> jobTypes, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        return startWorker(Arrays.asList(queueName), jobTypes, exceptionHandler, paused);
    }

    public Worker startWorker(List<String> queues, Map<String, Class<?>> jobTypes, ExceptionHandler exceptionHandler,
                              boolean paused)
    {
        JobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Worker worker = new GuiceAwareWorker(injector, jesqueConfig, queues, jobFactory);

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
        worker.getWorkerEventEmitter().addListener(new WorkerListener() {
            @Override
            public void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Exception ex) {
                if( event == WorkerEvent.WORKER_STOP ) {
                    workers.remove(worker);
                }
            }
        }, WorkerEvent.WORKER_STOP);

        vertx.runOnContext(new Handler<Void>() {
            @Override
            public void handle(Void event) {
                worker.run();
            }
        });
        return worker;
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
                long delay = jobCfg.getLong("deplay");
                delayEnqueue(delay, jobQueue, jobName, args);
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


    public void pauseAllWorkersOnThisNode() {
        LOG.info("Pausing all " + workers.size() + " jesque workers on this node");

        workers.forEach(worker -> {
            LOG.debug("Pausing worker processing queues: ${worker.queues}");
            worker.togglePause(true);
        });
    }

    public void resumeAllWorkersOnThisNode() {
        LOG.info("Resuming all " + workers.size() + " jesque workers on this node");

        workers.forEach(worker -> {
            LOG.debug("Pausing worker processing queues: ${worker.queues}");
            worker.togglePause(true);
        });
    }

}
