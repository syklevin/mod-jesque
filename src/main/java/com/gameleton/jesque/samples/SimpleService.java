package com.gameleton.jesque.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by levin on 8/22/2014.
 */
public class SimpleService {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleService.class);

    public void serve() {
        LOG.info("Heya! I am not here to serve, just to show you how injection works.");
    }

}
