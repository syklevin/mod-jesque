package com.gameleton.jesque;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by levin on 8/19/2014.
 */
public class JesqueAppVerticle extends Verticle {

    List<String> deployed = new ArrayList<>();

    @Override
    public void start() {
        final JsonObject config = container.config();

        //container.logger().info(config.toString());

        container.deployWorkerVerticle("com.gameleton.jesque.JesqueVerticle", config, 1, true, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> event) {
                if(event.succeeded()){
                    deployed.add(event.result());
                }
            }
        });

//        container.deployVerticle("com.gameleton.jesque.JesqueVerticle", config, 1, new AsyncResultHandler<String>() {
//            @Override
//            public void handle(AsyncResult<String> event) {
//                if(event.succeeded()){
//                    deployed.add(event.result());
//                }
//            }
//        });

//        container.deployVerticle("com.gameleton.jesque.JesqueWebVerticle", config, 1);
    }

    @Override
    public void stop() {
        for (String deployId : deployed) {
            container.undeployVerticle(deployId);
        }
        deployed.clear();
    }
}
