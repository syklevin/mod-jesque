package com.gameleton.jesque.scheduled;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by levin on 8/15/2014.
 */
public class JesqueScheduleRunner {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueScheduleRunner.class);
    protected static final long IDLE_WAIT_TIME = 10 * 1000;
    protected AtomicReference<JesqueScheduleThreadState> threadState = new AtomicReference(JesqueScheduleThreadState.New);
    private final JesqueScheduleService schedulerService;
    private final Vertx vertx;
    protected static String hostName;
    protected long timerId;

    public JesqueScheduleRunner(JesqueScheduleService schedulerService, Vertx vertx, String hostName){
        this.schedulerService = schedulerService;
        this.vertx = vertx;
        this.hostName = ensureHostName(hostName);
        this.timerId = -1;
    }

    public JesqueScheduleRunner(JesqueScheduleService schedulerService, Vertx vertx){
        this.schedulerService = schedulerService;
        this.vertx = vertx;
        this.hostName = ensureHostName(null);
        this.timerId = -1;
    }

    public void start(){
        LOG.info("Starting the jesque scheduled job runner");
        if( !threadState.compareAndSet(JesqueScheduleThreadState.New, JesqueScheduleThreadState.Running)) {
            LOG.error("Cannot start schedule runner twice, state was not the expected " + JesqueScheduleThreadState.New);
            return;
        }
        mainLoop();
    }

    public void stop(){
        threadState.set(JesqueScheduleThreadState.Stopped);
        if(timerId != -1){
            this.vertx.cancelTimer(timerId);
            timerId = -1;
        }
    }

    private void mainLoop(){
        DateTime now = new DateTime();
        schedulerService.processScheduledJobs(now, hostName);
        timerId = this.vertx.setPeriodic(IDLE_WAIT_TIME, new Handler<Long>() {
            @Override
            public void handle(Long delta) {
                DateTime findJobsUntil = new DateTime().plusMillis(delta.intValue());
                schedulerService.processScheduledJobs(findJobsUntil, hostName);
            }
        });
    }

    private String ensureHostName(String hostName) {
        if( hostName == null ) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostName = "localhost";
            }
        }
        return hostName;
    }
}
