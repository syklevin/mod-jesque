package com.gameleton.jesque.scheduled;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gameleton.jesque.JesqueService;
import com.gameleton.jesque.util.CronExpression;
import com.gameleton.jesque.trigger.Trigger;
import com.gameleton.jesque.trigger.TriggerField;
import com.gameleton.jesque.trigger.TriggerPersistor;
import com.gameleton.jesque.trigger.TriggerState;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.util.Pool;

import java.text.ParseException;
import java.util.*;

/**
 * Created by levin on 8/15/2014.
 */
public class JesqueScheduleService {

    public static final Logger LOG = LoggerFactory.getLogger(JesqueScheduleService.class);

    public static final String SCHEDULER_PREFIX = "scheduler";

    protected static final Integer STALE_SERVER_SECONDS = 30;
    private final JesqueService service;
    private final Pool<Jedis> pool;
    private final Vertx vertx;

    private final String namespace;
    
    public Vertx vertx() {
      return vertx;
    }

    public JesqueScheduleService(JesqueService service){
        this.service = service;
        this.vertx = service.vertx();
        this.pool = service.pool();
        this.namespace = service.jesqueConfig().getNamespace();
    }

    public void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, Object... args) {
        schedule(jobName, cronExpressionString, jesqueJobQueue, jesqueJobName, Arrays.asList(args));
    }

    public void schedule(String jobName, String cronExpressionString, String jesqueJobQueue, String jesqueJobName, List args) {
        schedule(jobName, cronExpressionString, DateTimeZone.getDefault(), jesqueJobQueue, jesqueJobName, args);
    }

    public void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, Object... args) {
        schedule(jobName, cronExpressionString, timeZone, jesqueJobQueue, jesqueJobName, Arrays.asList(args));
    }

    public void schedule(String jobName, String cronExpressionString, DateTimeZone timeZone, String jesqueJobQueue, String jesqueJobName, List args) {

        CronExpression cronExpression = null;
        try {
            cronExpression = new CronExpression(cronExpressionString, timeZone);
        } catch (ParseException ex) {
            LOG.error("Failed to add schedule job", ex);
            return;
        }

        Jedis redis = pool.getResource();

        //add schedule
        ScheduledJob scheduledJob = new ScheduledJob();
        scheduledJob.setName(jobName);
        scheduledJob.setCronExpression(cronExpressionString);
        scheduledJob.setArgs(args);
        scheduledJob.setJesqueJobName(jesqueJobName);
        scheduledJob.setJesqueJobQueue(jesqueJobQueue);
        scheduledJob.setTimeZone(timeZone);

        ScheduledJobPersistor.save(redis, scheduledJob);

        //add trigger state
        Trigger trigger = new Trigger();
        trigger.setJobName(jobName);
        trigger.setNextFireTime(cronExpression.getNextValidTimeAfter(new DateTime()));
        trigger.setState(TriggerState.Waiting);
        trigger.setAcquiredBy("");

        TriggerPersistor.save(redis, trigger);

        //update trigger indexes
        redis.zadd(Trigger.TRIGGER_NEXTFIRE_INDEX, trigger.getNextFireTime().getMillis(), jobName);

        pool.returnResource(redis);

    }

    public void deleteSchedule(String name) {
        Jedis redis = pool.getResource();
        ScheduledJobPersistor.delete(redis, name);
        TriggerPersistor.delete(redis, name);
        pool.returnResource(redis);
    }

    void enqueueReadyJobs(DateTime until, final String hostName) {
        LOG.info("enqueueReadyJobs at time " + until.toString());
        //check to see if there are any servers that have missed removing check-in
        //if so get the intersection of WATIING jobs that are not in the nextFireTimeIndex and add
        final List<Tuple> acquiredJobs = acquireJobs(until, 1, hostName);


        LOG.info("acquiredJobs size: " + acquiredJobs.size());

        if( acquiredJobs == null || acquiredJobs.size() == 0)
            return;

        long earliestAcquiredJobTime = (long)acquiredJobs.get(0).getScore();

        long delta = earliestAcquiredJobTime - DateTime.now().getMillis();

        LOG.info("earliestAcquiredJobTime " + earliestAcquiredJobTime + ", delta " + delta);

        if( delta > 0 ) {
            LOG.debug("Waiting for fire time to enqueue jobs");

            vertx.setTimer(delta, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    enqueueJobs(acquiredJobs, hostName);
                }
            });
        }
        else{
            enqueueJobs(acquiredJobs, hostName);
        }
    }

    private void enqueueJobs(List<Tuple> acquiredJobs, String hostName){

        LOG.info("enqueueJobs acquiredJobs size " + acquiredJobs.size() + ", hostName " + hostName);

        Jedis redis = pool.getResource();
        for (Tuple acquiredJob : acquiredJobs) {
            enqueueJob(redis, acquiredJob.getElement(), hostName);
        }
        pool.returnResource(redis);
    }

    private List<Tuple> acquireJobs(DateTime until, Integer number, String hostName) {

        Jedis redis = pool.getResource();

        //retrieving min time trigger
        Set<Tuple> allJobs = redis.zrangeWithScores(Trigger.TRIGGER_NEXTFIRE_INDEX, 0, number - 1);

        LOG.info("allJobs size " + allJobs.size());

        Set<Tuple> waitingJobs = new HashSet<>();

        for (Tuple job : allJobs) {
            if(job.getScore() <= until.getMillis()){
                waitingJobs.add(job);
            }
        }

        if( waitingJobs.size() <= 0 )
            return new ArrayList<>();

        //only get and wait for jobs with the exact time until something like quartz batchTimeWindow is implemented
        Tuple earliestWaitingJobTime = Collections.min(waitingJobs, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                return (int) (o2.getScore() - o1.getScore());
            }
        });

        List<Tuple> acquiredJobs = new ArrayList<>();

        for (Tuple waitingJob : waitingJobs) {

            //filter again for earliestWaitingJobTime
            if(waitingJob.getScore() == earliestWaitingJobTime.getScore()){
                String jobName = waitingJob.getElement();

                redis.watch(Trigger.getRedisKeyForJobName(jobName));
                Trigger trigger = TriggerPersistor.findByJobName(redis, jobName);
                if( trigger.getState() != TriggerState.Waiting ) {
                    LOG.debug("Trigger not in waiting state for job " + jobName);
                    redis.unwatch();
                }

                trigger.setState(TriggerState.Acquired);
                trigger.setAcquiredBy(hostName);
                Transaction transaction = redis.multi();
                List<Object> transactionResult = null;
                try{
                    transaction.hmset(trigger.getRedisKey(), trigger.toRedisHash());
                    transaction.zrem(Trigger.TRIGGER_NEXTFIRE_INDEX, jobName);
                    transaction.sadd(Trigger.getAcquiredIndexByHostName(hostName), jobName);
                    transactionResult = transaction.exec();
                } catch (Exception exception) {
                    LOG.error("Exception setting trigger state, discarding transaction", exception);
                    transaction.discard();
                }

                if( transactionResult != null )
                    acquiredJobs.add(waitingJob);
                else
                    LOG.debug("Could not acquire job ${jobName} due to trigger state change");
            }
        }

        return acquiredJobs;
    }

    private void enqueueJob(Jedis redis, String jobName, String hostName) {
        LOG.info("Enqueuing job " + jobName);
        redis.watch(Trigger.getRedisKeyForJobName(jobName));
        ScheduledJob scheduledJob = ScheduledJobPersistor.findByName(redis, jobName);
        Trigger trigger = scheduledJob.getTrigger();
        if( trigger.getState() != TriggerState.Acquired ) {
            LOG.warn("Trigger state is no longer " + TriggerState.Acquired.name() + " for job " + jobName);
            redis.srem(Trigger.getAcquiredIndexByHostName(hostName), jobName);
            redis.unwatch();
            return;
        }
        if(!hostName.equals(trigger.getAcquiredBy())) {
            LOG.warn("Trigger state was acquired by another server " + trigger.getAcquiredBy() +" for job " + jobName);
            redis.srem(Trigger.getAcquiredIndexByHostName(hostName), jobName);
            redis.unwatch();
            return;
        }

        CronExpression cronExpression = null;
        try {
            cronExpression = new CronExpression(scheduledJob.getCronExpression(), scheduledJob.getTimeZone());
        } catch (ParseException e) {
            LOG.error("enqueueJob failed due to invalid cronExpression");
            return;
        }

        trigger.setNextFireTime(cronExpression.getNextValidTimeAfter(new DateTime()));
        trigger.setState(TriggerState.Waiting);
        trigger.setAcquiredBy("");

        Job job = new Job(scheduledJob.getJesqueJobName(), scheduledJob.getArgs());

        String jobJson = null;
        try {
            jobJson = ObjectMapperFactory.get().writeValueAsString(job);
        } catch (JsonProcessingException e) {
            LOG.error("enqueueJob failed due to invalid job json");
            return;
        }

        Transaction transaction = redis.multi();
        try{
            transaction.sadd(service.jesqueConfig().getNamespace() + ":" + ResqueConstants.QUEUES, scheduledJob.getJesqueJobQueue());
            transaction.rpush(getRedisKeyForQueueName(scheduledJob.getJesqueJobQueue()), jobJson);
            transaction.hmset(trigger.getRedisKey(), trigger.toRedisHash());
            transaction.zadd(Trigger.TRIGGER_NEXTFIRE_INDEX, trigger.getNextFireTime().getMillis(), jobName);
            if( transaction.exec() == null ) {
                LOG.warn("Job exection aborted for " + jobName + " because the trigger data changed");
            }
        } catch(Exception exception) {
            LOG.error("Exception enqueuing job, discarding transaction", exception);
            transaction.discard();
        }

        //always remove index regardless of the result of the enqueue
        redis.srem(Trigger.getAcquiredIndexByHostName(hostName), jobName);
    }

    void serverCheckIn(String hostName, DateTime checkInDate) {
        Jedis redis = null;
        try {
            redis = pool.getResource();
            //TODO: detect checkins by another server of the same name

            String key = JesqueUtils.createKey(namespace, SCHEDULER_PREFIX, "checkIn");

            redis.hset(key, hostName, String.valueOf(checkInDate.getMillis()));
        }
        finally {
            pool.returnResource(redis);
        }

    }

    void cleanUpStaleServers() {
        Jedis redis = pool.getResource();
        DateTime now = new DateTime();

        String key = JesqueUtils.createKey(namespace, SCHEDULER_PREFIX, "checkIn");

        Map<String, String> serverCheckInHash = redis.hgetAll(key);
        for (Map.Entry<String, String> entry : serverCheckInHash.entrySet()) {
            Seconds sec = Seconds.secondsBetween(new DateTime(Long.parseLong(entry.getValue())), now);
            if(sec.getSeconds() > STALE_SERVER_SECONDS){
                cleanUpStaleServer(redis, entry.getKey());
            }
        }
        pool.returnResource(redis);
    }

    private void cleanUpStaleServer(Jedis redis, String hostName) {
        LOG.info("Cleaning up stale server $hostName");
        String staleServerAcquiredJobsSetName = Trigger.getAcquiredIndexByHostName(hostName);
        Set<String> staleJobNames = redis.smembers(staleServerAcquiredJobsSetName);
        Map<String, Long> triggerFireTime = new HashMap<>();
        String key;
        for (String staleJobName : staleJobNames) {
            key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, staleJobName);
            String fireTimeString = redis.hget(key, TriggerField.NextFireTime.name());
            triggerFireTime.put(staleJobName, Long.parseLong(fireTimeString));
        }
        redis.watch(staleServerAcquiredJobsSetName);
        Transaction transaction = redis.multi();


        try{
            //put jobs back to redis

            for (String staleJobName : staleJobNames) {
                key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, staleJobName);
                transaction.hset(key, TriggerField.State.name(), TriggerState.Waiting.name());
                transaction.hset(key, TriggerField.AcquiredBy.name(), "");
                transaction.zadd(JesqueUtils.createKey(namespace, Trigger.TRIGGER_NEXTFIRE_INDEX), triggerFireTime.get(staleJobName), staleJobName);
            }

            key = JesqueUtils.createKey(namespace, SCHEDULER_PREFIX, "checkIn");

            transaction.del(staleServerAcquiredJobsSetName);
            transaction.hdel(key, hostName);
            transaction.exec();
        } catch (Exception exception) {
            LOG.error("Exception cleaning up stale servers, discarding transaction", exception);
            transaction.discard();
        }
    }

    public String getRedisKeyForQueueName(String queueName) {
        return service.jesqueConfig().getNamespace() + ":" + ResqueConstants.QUEUE + ":" + queueName;
    }




}
