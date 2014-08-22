package com.gameleton.jesque;

import com.gameleton.jesque.admin.JesqueAdminServiceImpl;
import com.gameleton.jesque.impl.JesqueModule;
import com.gameleton.jesque.impl.JesqueService;
import com.gameleton.jesque.samples.SimpleModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
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

    protected Injector setupGuice(Module... modules) {
        List<Module> allModules = new ArrayList<>();
        allModules.add(new JesqueModule(vertx));
        for (Module m : modules) {
            allModules.add(m);
        }
        Injector injector = Guice.createInjector(allModules);
        return injector;
    }

    @Override
    public void start() {

        Injector injector = setupGuice(new SimpleModule());
        jesqueService = injector.getInstance(JesqueService.class);
        jesqueService.configure(container.config());
        jesqueService.start();

        //jesqueAdminServiceImpl = new JesqueAdminServiceImpl(vertx, config);
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
