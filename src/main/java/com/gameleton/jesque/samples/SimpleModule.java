package com.gameleton.jesque.samples;

import com.google.inject.AbstractModule;

/**
 * Created by levin on 8/22/2014.
 */
public class SimpleModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SimpleService.class).asEagerSingleton();
    }
}
