package com.gameleton.jesque;

import com.gameleton.jesque.admin.JesqueAdminServiceImpl;
import com.gameleton.jesque.impl.JesqueServiceImpl;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.worker.*;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.*;

/**
 * Created by levin on 7/23/2014.
 */
public class JesqueVerticle extends Verticle implements Handler<Message<JsonObject>> {

    private JesqueService jesqueService;
    private JesqueAdminServiceImpl jesqueAdminServiceImpl;

    @Override
    public void start() {

        JsonObject config = container.config();

        jesqueService = new JesqueServiceImpl(vertx, config);
        jesqueAdminServiceImpl = new JesqueAdminServiceImpl(vertx, config);
    }

    @Override
    public void stop() {
        if(jesqueService != null){
            jesqueService.stop();
        }
    }

    @Override
    public void handle(Message<JsonObject> message) {
//        String action = getMandatoryString("action", message);
//        switch (action){
//            case "addJob":
//                //doAddJob(message);
//                break;
//            case "removeJob":
//                //doRemoveJob(message);
//                break;
//        }
    }

}
