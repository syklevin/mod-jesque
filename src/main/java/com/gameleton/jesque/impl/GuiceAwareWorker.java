package com.gameleton.jesque.impl;

import com.google.inject.Injector;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.JobFactory;
import net.greghaines.jesque.worker.WorkerImpl;

import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;


/**
 * Created by levin on 8/22/2014.
 */
public class GuiceAwareWorker extends WorkerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(GuiceAwareWorker.class);

    /** The injector. */
    final Injector injector;

    @SuppressWarnings("unchecked")
    @Inject
    public GuiceAwareWorker(final Injector injector, final Config config, Collection<String> queues, JobFactory jobFactory) {
        super(config, queues, jobFactory);
        this.injector = injector;
    }

    @Override
    protected Object execute(Job job, String curQueue, Object instance) throws Exception {
        LOG.debug("Injecting dependencies into worker instance = {}", instance);
        injector.injectMembers(instance);
        return super.execute(job, curQueue, instance);
    }
}
