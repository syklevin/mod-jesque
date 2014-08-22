package com.gameleton.jesque.deplayed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gameleton.jesque.impl.JesqueService;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.util.Pool;

import java.io.IOException;
import java.util.Set;

/**
 * Created by levin on 8/14/2014.
 */
public class JesqueDelayedJobService {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueDelayedJobService.class);

    protected static final String RESQUE_DELAYED_JOBS_PREFIX = "resque:delayed";
    private final JesqueService jesqueService;

    private Pool<Jedis> pool;

    public JesqueDelayedJobService(JesqueService jesqueService){
        this.jesqueService = jesqueService;
        this.pool = jesqueService.pool();
    }

    public void enqueueAt(DateTime at, String queueName, Job job) throws JsonProcessingException {
        String jobString = ObjectMapperFactory.get().writeValueAsString(job);
        Pipeline pipeline = pool.getResource().pipelined();
        long currMillis = System.currentTimeMillis();
        pipeline.rpush(RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName + ":" + currMillis, jobString );
        pipeline.zadd(RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName, currMillis, String.valueOf(currMillis));
        pipeline.sadd(RESQUE_DELAYED_JOBS_PREFIX + ":queues", queueName);
        pipeline.sync();
    }

    public void enqueueReadyJobs() {
        Jedis redis = null;
        try {
            redis = pool.getResource();
            long maxScore = new DateTime().getMillis();
            Set<String> queueNames = redis.smembers(RESQUE_DELAYED_JOBS_PREFIX + ":queues");
            for (String queueName : queueNames) {
                Set<String> jobsTimestamps = redis.zrangeByScore(RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName, 0, maxScore);
                for (String jobsTimestamp : jobsTimestamps) {
                    String jobString = redis.lpop(RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName + ":" + jobsTimestamp);
                    if (jobString != null) {
                        try {
                            Job job = ObjectMapperFactory.get().readValue(jobString, Job.class);
                            jesqueService.enqueue(queueName, job);
                        } catch (IOException ex) {
                            LOG.error("enqueueReadyJobs exception of " + jobString, ex);
                        }
                    } else {
                        deleteQueueTimestampListIfEmpty(queueName, jobsTimestamp);
                    }
                }
            }
        }
        finally {
            pool.returnResource(redis);
        }
    }

    public DateTime nextFireTime() {
        Jedis redis = null;
        try {
            redis = pool.getResource();
            Set<String> queueNames = redis.smembers(RESQUE_DELAYED_JOBS_PREFIX + ":queues");
            long minTimestamp = Long.MAX_VALUE;
            for (String queueName : queueNames) {
                Set<String> timestamps = redis.zrangeByScore(RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName, "0", "inf", 0, 1 );
                if(timestamps != null && timestamps.size() > 0) {
                    minTimestamp = Long.parseLong(timestamps.iterator().next());
                    break; //break the loop
                }
            }
            return new DateTime( minTimestamp );
        }
        finally {
            pool.returnResource(redis);
        }
    }

    protected void deleteQueueTimestampListIfEmpty(String queueName, String timestamp) {
        String queueTimestampKey = RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName + ":" + timestamp;

        Jedis redis = null;
        try {
            redis = pool.getResource();

            redis.watch(queueTimestampKey);
            long length = redis.llen( queueTimestampKey);

            if( length == 0 ) {
                Transaction transaction = redis.multi();
                transaction.del( queueTimestampKey);
                transaction.zrem(RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName, timestamp);
                transaction.exec();
            } else {
                redis.unwatch();
            }
        }
        finally {
            pool.returnResource(redis);
        }
    }

    protected void deleteDelayedQueueIfEmpty(String queueName) {
        String queueKey = RESQUE_DELAYED_JOBS_PREFIX + ":" + queueName;

        Jedis redis = null;
        try {
            redis = pool.getResource();

            redis.watch(queueKey);
            long length = redis.zcard(queueKey);

            if( length == 0) {
                Transaction transaction = redis.multi();
                transaction.del(queueKey);
                transaction.srem(RESQUE_DELAYED_JOBS_PREFIX + ":queues", queueName);
                transaction.exec();
            } else {
                redis.unwatch();
            }
        }
        finally {
            pool.returnResource(redis);
        }
    }

}
