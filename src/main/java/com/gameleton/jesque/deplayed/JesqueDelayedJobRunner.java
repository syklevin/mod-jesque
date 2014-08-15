package com.gameleton.jesque.deplayed;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by levin on 8/14/2014.
 */
public class JesqueDelayedJobRunner {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueDelayedJobRunner.class);

    private final Vertx vertx;
    private final JesqueDelayedJobService jesqueDelayedJobService;
    protected AtomicReference<JesqueDelayedJobThreadState> threadState = new AtomicReference(JesqueDelayedJobThreadState.New);

    protected long maxDelayMillis;

    protected long timerId;

    public JesqueDelayedJobRunner(JesqueDelayedJobService jesqueDelayedJobService, Vertx vertx, long maxDelayMillis){
        this.jesqueDelayedJobService = jesqueDelayedJobService;
        this.vertx = vertx;
        this.maxDelayMillis = 2000;
    }

    public void start() {
        LOG.info("Starting the jesque delayed job runner");
        if( !threadState.compareAndSet(JesqueDelayedJobThreadState.New, JesqueDelayedJobThreadState.Running)) {
            LOG.error("Cannot start delayed job thread, state was not the expected " + JesqueDelayedJobThreadState.New.toString());
            return;
        }
        mainLoop();
    }

    public void stop() {
        LOG.info("Stopping the jesque delayed job thread");
        threadState.set(JesqueDelayedJobThreadState.Stopped);
        if(timerId != -1){
            this.vertx.cancelTimer(timerId);
            timerId = -1;
        }
    }

    private void mainLoop() {
        jesqueDelayedJobService.enqueueReadyJobs();
        DateTime nextFireTime = jesqueDelayedJobService.nextFireTime();
        long timeToNextJobMs = nextFireTime.getMillis() - new DateTime().getMillis();
        long nextFrameDelay;
        if (timeToNextJobMs < maxDelayMillis ){
            nextFrameDelay = timeToNextJobMs;
        }
        else{
            nextFrameDelay = maxDelayMillis;
        }
        timerId = vertx.setTimer(nextFrameDelay, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                mainLoop();
            }
        });
    }
}
