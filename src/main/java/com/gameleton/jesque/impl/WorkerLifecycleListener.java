package com.gameleton.jesque.impl;

import com.gameleton.jesque.JesqueService;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by levin on 8/15/2014.
 */
public class WorkerLifecycleListener implements WorkerListener {

    public static final Logger LOG = LoggerFactory.getLogger(WorkerLifecycleListener.class);

    private final JesqueService jesqueService;

    WorkerLifecycleListener(JesqueService jesqueService) {
        this.jesqueService = jesqueService;
    }

    @Override
    public void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Exception ex) {
        if( event == WorkerEvent.WORKER_STOP ) {
            jesqueService.removeWorkerFromLifecycleTracking(worker);
        }

    }
}
