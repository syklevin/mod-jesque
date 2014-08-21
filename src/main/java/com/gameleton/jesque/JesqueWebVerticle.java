package com.gameleton.jesque;

import com.gameleton.jesque.admin.JesqueAdminServiceImpl;
import com.gameleton.jesque.impl.JesqueServiceImpl;
import com.gameleton.jesque.util.StringUtils;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.worker.*;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.util.*;

/**
 * Created by levin on 7/23/2014.
 */
public class JesqueWebVerticle extends Verticle {

    private HttpServer httpServer;

    @Override
    public void start() {

        final JsonObject config = container.config();
        final Logger LOG = container.logger();

        final JesqueAdminServiceImpl jesqueAdminServiceImpl = new JesqueAdminServiceImpl(vertx, config);

        if(config.containsField("jesqueWeb")){
            JsonObject jesqueWebCfg = config.getObject("jesqueWeb");
            httpServer = vertx.createHttpServer();
            RouteMatcher matcher = new RouteMatcher();
            matcher.get("/app/:files", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    String files = req.params().get("files");
                    req.response().sendFile("app/" + files);
                }
            });

            matcher.get("/overview", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    req.response().end(jesqueAdminServiceImpl.getOverview().toString());
                }
            });

            matcher.get("/failed", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    Map<String, String> queryMap = StringUtils.getQueryMap(req.query());
                    req.response().end(jesqueAdminServiceImpl.getFailed(queryMap.get("offset"), queryMap.get("count")).toString());
                }
            });

            matcher.get("/key/:id", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    Map<String, String> queryMap = StringUtils.getQueryMap(req.query());
                    String id = req.params().get("id");
                    req.response().end(jesqueAdminServiceImpl.getKeys(id, queryMap.get("offset"), queryMap.get("count")).toString());
                }
            });

            matcher.noMatch(new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest req) {
                    req.response().setStatusCode(403);
                    req.response().end();
                    req.response().close();
                }
            });

            final int port = jesqueWebCfg.getInteger("port", 9898);
            final String host = jesqueWebCfg.getString("host", "localhost");

            httpServer.setReuseAddress(true);

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

    @Override
    public void stop() {

        if(httpServer != null){
            httpServer.close();
        }
    }



}
