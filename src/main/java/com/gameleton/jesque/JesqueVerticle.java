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

import java.util.*;

/**
 * Created by levin on 7/23/2014.
 */
public class JesqueVerticle extends BusModBase implements Handler<Message<JsonObject>> {

    private JesqueService jesqueService;
    private JesqueAdminServiceImpl jesqueAdminServiceImpl;

    @Override
    public void start() {
        jesqueService = new JesqueServiceImpl(vertx, config);
        jesqueAdminServiceImpl = new JesqueAdminServiceImpl(vertx, config);
    }

    @Override
    public void stop() {
        if(jesqueService != null){
            jesqueService.stop();
        }
        if(jesqueAdminServiceImpl != null){
            jesqueAdminServiceImpl.stop();
        }
    }

    @Override
    public void handle(Message<JsonObject> message) {
        String action = getMandatoryString("action", message);
        switch (action){
            case "addJob":
                //doAddJob(message);
                break;
            case "removeJob":
                //doRemoveJob(message);
                break;
        }
    }

//    private void doAddJob(Message<JsonObject> message){
//        String jobClass = getMandatoryString("job", message);
//        String queue = getMandatoryString("queue", message);
//        Job job = new Job(jobClass);
//
//        if(message.body().containsField("args")){
//            Map<String, Object> args = message.body().getObject("args").toMap();
//            job.setVars(args);
//        }
//
//        Client jesqueClient = new ClientImpl(jesqueConfig);
//        jesqueClient.enqueue(queue, job);
//        jesqueClient.end();
////        Worker jesqueWorker = new WorkerImpl(jesqueConfig, Arrays.asList(queue), new ReflectiveJobFactory());
////        runWorker(jesqueWorker);
//        //workers.put(queue, jesqueWorker);
//        sendOK(message);
//    }
//
//    private void doRemoveJob(Message<JsonObject> message){
//        String queue = getMandatoryString("queue", message);
//        //Worker jesqueWorker = workers.remove(queue);
////        if(jesqueWorker != null){
////            jesqueWorker.end(true);
////        }
//        sendOK(message);
//    }


}
