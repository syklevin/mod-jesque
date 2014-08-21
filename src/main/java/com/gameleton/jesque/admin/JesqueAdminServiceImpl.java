package com.gameleton.jesque.admin;

import com.gameleton.jesque.scheduled.ScheduledJob;
import com.gameleton.jesque.scheduled.ScheduledJobPersistor;
import com.gameleton.jesque.util.ResqueDataParser;
import com.gameleton.jesque.util.StringUtils;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.meta.KeyInfo;
import net.greghaines.jesque.meta.QueueInfo;
import net.greghaines.jesque.meta.WorkerInfo;
import net.greghaines.jesque.meta.dao.FailureDAO;
import net.greghaines.jesque.meta.dao.KeysDAO;
import net.greghaines.jesque.meta.dao.QueueInfoDAO;
import net.greghaines.jesque.meta.dao.WorkerInfoDAO;
import net.greghaines.jesque.meta.dao.impl.FailureDAORedisImpl;
import net.greghaines.jesque.meta.dao.impl.KeysDAORedisImpl;
import net.greghaines.jesque.meta.dao.impl.QueueInfoDAORedisImpl;
import net.greghaines.jesque.meta.dao.impl.WorkerInfoDAORedisImpl;
import net.greghaines.jesque.utils.PoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.List;
import java.util.Map;

/**
 * Created by levin on 8/15/2014.
 */
public class JesqueAdminServiceImpl {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueAdminServiceImpl.class);

    private final Vertx vertx;
    private final JsonObject config;
    private final Config jesqueConfig;
    private final Pool<Jedis> pool;

    private FailureDAO failureDAO;
    private KeysDAO keysDAO;
    private QueueInfoDAO queueInfoDAO;
    private WorkerInfoDAO workerInfoDAO;

    public JesqueAdminServiceImpl(Vertx vertx, JsonObject config){
        this.vertx = vertx;
        this.config = config;

        ConfigBuilder builder = new ConfigBuilder();

        JsonObject jesqueCfg = config.getObject("jesque");

        builder.withHost(jesqueCfg.getString("redis_host"))
                .withPort(jesqueCfg.getInteger("redis_port"))
                .withNamespace(jesqueCfg.getString("redis_namespace"));

        this.jesqueConfig = builder.build();
        this.pool = PoolUtils.createJedisPool(jesqueConfig);

        failureDAO = new FailureDAORedisImpl(jesqueConfig, pool);
        keysDAO = new KeysDAORedisImpl(jesqueConfig, pool);
        queueInfoDAO = new QueueInfoDAORedisImpl(jesqueConfig, pool);
        workerInfoDAO = new WorkerInfoDAORedisImpl(jesqueConfig, pool);
    }

    public JsonObject getOverview(){
        JsonObject rootNode = new JsonObject();
        JsonArray queues = new JsonArray();
        for(QueueInfo info : queueInfoDAO.getQueueInfos()){
            queues.add(ResqueDataParser.parseQueueInfo(info));
        }

        JsonArray workers = new JsonArray();
        for(WorkerInfo worker : workerInfoDAO.getActiveWorkers()){
            queues.add(ResqueDataParser.parseWorkerInfo(worker));
        }

        rootNode.putArray("queues", queues);
        rootNode.putNumber("totalFailureCount", failureDAO.getCount());
        rootNode.putArray("working", workers);
        rootNode.putNumber("totalWorkerCount", workerInfoDAO.getWorkerCount());

        return rootNode;
    }

    public JsonObject getFailed(String offset, String count){
        long offsetVal = 0;
        long countVal = 20;
        if(offset != null){
            offsetVal = Long.parseLong(offset);
        }
        if(count != null){
            countVal = Long.parseLong(count);
        }
        JsonObject rootNode = new JsonObject();
        JsonArray failures = new JsonArray();
        for (JobFailure failure : failureDAO.getFailures(offsetVal, countVal)) {
            failures.add(ResqueDataParser.parseJobFailure(failure));
        }
        rootNode.putNumber("fullFailureCount", failureDAO.getCount());
        rootNode.putArray("failures", failures);

        return rootNode;
    }

    public JsonObject getQueue(String queueName, String offset, String count){
        long offsetVal = 0;
        long countVal = 20;
        if(offset != null){
            offsetVal = Long.parseLong(offset);
        }
        if(count != null){
            countVal = Long.parseLong(count);
        }
        JsonObject rootNode = new JsonObject();
        QueueInfo queueInfo = queueInfoDAO.getQueueInfo(queueName, offsetVal, countVal);
        rootNode.putObject("queue", ResqueDataParser.parseQueueInfo(queueInfo));
        rootNode.putNumber("total", queueInfo.getSize());
        return rootNode;
    }

    public JsonObject getWorker(String workerName){
        JsonObject rootNode = new JsonObject();
        WorkerInfo workerInfo = workerInfoDAO.getWorker(workerName);
        rootNode.putObject("worker", ResqueDataParser.parseWorkerInfo(workerInfo));
        return rootNode;
    }

    public JsonObject getRedis(String statType){
        JsonObject rootNode = new JsonObject();
        rootNode.putObject("redis", new JsonObject((Map)keysDAO.getRedisInfo()));
        return rootNode;
    }

    public JsonObject getKeys(String key, String offset, String count){
        int offsetVal = 0;
        int countVal = 20;
        if(offset != null){
            offsetVal = Integer.parseInt(offset);
        }
        if(count != null){
            countVal = Integer.parseInt(count);
        }
        JsonObject rootNode = new JsonObject();
        KeyInfo keyInfo = keysDAO.getKeyInfo(jesqueConfig.getNamespace() + ':' + key, offsetVal, countVal);

        if(keyInfo == null){
            rootNode.putString("error", "given key not found");
        }
        else{
            rootNode.putObject("key", ResqueDataParser.parseKeyInfo(keyInfo));
            rootNode.putNumber("total", keyInfo.getSize());
        }

        return rootNode;
    }

    public JsonObject getScheduled(String statType){
        Jedis redis = pool.getResource();
        List<ScheduledJob> ScheduledJobs = ScheduledJobPersistor.getAll(redis);
        pool.returnResource(redis);
        JsonObject rootNode = new JsonObject();
        JsonArray list = new JsonArray();
        for (ScheduledJob scheduledJob : ScheduledJobs) {
            list.add(ResqueDataParser.parseScheduledJob(scheduledJob));
        }
        rootNode.putArray("scheduledJobs", list);
        return rootNode;
    }
}
