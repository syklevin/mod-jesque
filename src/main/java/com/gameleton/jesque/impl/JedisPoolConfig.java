package com.gameleton.jesque.impl;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by levin on 8/19/2014.
 */
public class JedisPoolConfig extends GenericObjectPoolConfig {

    public JedisPoolConfig() {
        // defaults to make your life with connection pool easier :)
        setTestWhileIdle(true);
        setMinEvictableIdleTimeMillis(60000);
        setTimeBetweenEvictionRunsMillis(30000);
        setNumTestsPerEvictionRun(-1);

        setMaxTotal(200);
        setMaxIdle(200);
        setMinIdle(100);
        setTestOnBorrow(true);

//        setMaxTotal(-1); // Infinite
//        setMaxIdle(10);
//        setMinIdle(1);
//        setTestOnBorrow(true);
//        setBlockWhenExhausted(false);

    }

}
