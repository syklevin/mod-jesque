package com.gameleton.jesque.trigger;


import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;

/**
 * Created by levin on 8/14/2014.
 */
public class TriggerDaoService {

    private Config config;
    private Pool<Jedis> jedisPool;

    public TriggerDaoService(Config config, Pool<Jedis> jedisPool){
        this.config = config;
        this.jedisPool = jedisPool;
    }

    public void save(final Trigger trigger) {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                save(jedis, trigger);
                return null;
            }
        });
    }

    public void save(final Jedis jedis, final Trigger trigger) {
        String key = JesqueUtils.createKey(config.getNamespace(), Trigger.TRIGGER_PREFIX, trigger.getJobName());
        jedis.hmset(key, trigger.toRedisHash());
    }

    public void delete(final String jobName) {

        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                delete(jedis, jobName);
                return null;
            }
        });
    }

    public void delete(final Jedis jedis, final String jobName) {
        String key = JesqueUtils.createKey(config.getNamespace(), Trigger.TRIGGER_PREFIX, jobName);
        jedis.del(key);
        key = JesqueUtils.createKey(config.getNamespace(), Trigger.TRIGGER_NEXTFIRE_INDEX);
        jedis.zrem(key, jobName);
    }

    public Trigger findByJobName(final String jobName) {
        return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, Trigger>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Trigger doWork(final Jedis jedis) throws Exception {
                return findByJobName(jedis, jobName);
            }
        });
    }

    public Trigger findByJobName(final Jedis jedis, final String jobName) {
        String key = JesqueUtils.createKey(config.getNamespace(), Trigger.TRIGGER_PREFIX, jobName);
        return Trigger.fromRedisHash(jedis.hgetAll(key));
    }

}
