package com.gameleton.jesque.samples;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Created by levin on 8/15/2014.
 */
public class HelloJob implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(HelloJob.class);

    @Inject
    private SimpleService service;

    @Override
    public void run() {
        service.serve();
        //LOG.info("HelloJob " + String.valueOf(System.currentTimeMillis()));
    }
}
