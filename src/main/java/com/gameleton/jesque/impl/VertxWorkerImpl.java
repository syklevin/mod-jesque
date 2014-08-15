package com.gameleton.jesque.impl;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.JobFactory;
import net.greghaines.jesque.worker.WorkerAware;
import net.greghaines.jesque.worker.WorkerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

import java.util.Collection;
import java.util.concurrent.Callable;

import static net.greghaines.jesque.worker.WorkerEvent.JOB_EXECUTE;

/**
 * Created by levin on 8/15/2014.
 */
public class VertxWorkerImpl extends WorkerImpl {
    public static final Logger LOG = LoggerFactory.getLogger(VertxWorkerImpl.class);
    private final Vertx vertx;


    public VertxWorkerImpl(Vertx vertx, Config config, Collection<String> queues, JobFactory jobFactory) {
        super(config, queues, jobFactory);
        this.vertx = vertx;
    }

    @Override
    protected Object execute(Job job, String curQueue, Object instance) throws Exception {
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this);
        }
        this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null);
        final Object result;
        if (instance instanceof Callable) {
            result = ((Callable<?>) instance).call(); // The job is executing!
        } else if (instance instanceof Runnable) {

            final Runnable runnable = (Runnable) instance;

            vertx.runOnContext(new Handler<Void>() {
                @Override
                public void handle(Void event) {
                    runnable.run(); // The job is executing!
                }
            });

            result = null;
        } else { // Should never happen since we're testing the class earlier
            throw new ClassCastException("Instance must be a Runnable or a Callable: " + instance.getClass().getName()
                    + " - " + instance);
        }
        return result;
    }

}
