package com.gameleton.jesque.admin;

import com.gameleton.jesque.util.ResqueDataParser;
import com.gameleton.jesque.util.StringUtils;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.JobFailure;
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


    private HttpServer httpServer;

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

        if(config.containsField("jesqueWeb")){
            JsonObject jesqueWebCfg = config.getObject("jesqueWeb");
            httpServer = vertx.createHttpServer();
            final String dir = jesqueWebCfg.getString("dir");
            RouteMatcher matcher = new RouteMatcher();
            matcher.get("/", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    req.response().sendFile(dir + "/index.html");
                }
            });
            matcher.get("/js/:jsFile", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    String jsFileName = req.params().get("jsFile");
                    req.response().sendFile(dir + "/" + jsFileName);
                }
            });

            matcher.get("/overview", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    req.response().end(getOverview().toString());
                }
            });

            matcher.get("/failed", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    Map<String, String> queryMap = StringUtils.getQueryMap(req.query());
                    req.response().end(getFailed(queryMap.get("offset"), queryMap.get("count")).toString());
                }
            });

            final int port = jesqueWebCfg.getInteger("port", 9898);
            final String host = jesqueWebCfg.getString("host", "localhost");

            httpServer.requestHandler(matcher).listen(port, host, new AsyncResultHandler<HttpServer>() {
                @Override
                public void handle(AsyncResult<HttpServer> event) {
                    if(event.succeeded()){
                        LOG.info("Jesque Admin Http Service start successed on " + port);
                    }
                    else{
                        LOG.error("Jesque Admin Http Service start failed ", event.cause());
                    }
                }
            });
        }

    }

    public void stop(){
        if(httpServer != null){
            httpServer.close();
        }
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
}
