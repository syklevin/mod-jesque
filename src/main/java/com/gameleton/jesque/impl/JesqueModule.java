package com.gameleton.jesque.impl;

import com.google.inject.AbstractModule;
import org.vertx.java.core.Vertx;

/**
 * Created by levin on 8/22/2014.
 */
public class JesqueModule extends AbstractModule {

    private final Vertx vertx;

    public JesqueModule(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    protected void configure() {
        bind(Vertx.class).toInstance(vertx);
    }
}
