package com.gameleton.jesque.scheduled;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gameleton.jesque.JesqueService;
import com.gameleton.jesque.trigger.TriggerDaoService;
import com.gameleton.jesque.util.CronExpression;
import com.gameleton.jesque.trigger.Trigger;
import com.gameleton.jesque.trigger.TriggerField;
import com.gameleton.jesque.trigger.TriggerState;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final ScheduledJobDaoService scheduledJobDaoService;
    private final TriggerDaoService triggerDaoService;

    public Vertx vertx() {
      return vertx;
    }

    public JesqueScheduleService(JesqueService service){
        this.service = service;
        this.vertx = service.vertx();
        this.pool = service.pool();
        this.namespace = service.jesqueConfig().getNamespace();
        this.scheduledJobDaoService = new ScheduledJobDaoService(service.jesqueConfig(), service.pool());
        this.triggerDaoService = new TriggerDaoService(service.jesqueConfig(), service.pool());
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

    public void schedule(final String jobName, final String cronExpressionString, final DateTimeZone timeZone, final String jesqueJobQueue, final String jesqueJobName, final List args) {

        PoolUtils.doWorkInPoolNicely(this.pool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                CronExpression cronExpression = null;
                try {
                    cronExpression = new CronExpression(cronExpressionString, timeZone);
                } catch (ParseException ex) {
                    LOG.error("Failed to add schedule job", ex);
                    throw ex;
                }

                //add schedule
                ScheduledJob scheduledJob = new ScheduledJob();
                scheduledJob.setName(jobName);
                scheduledJob.setCronExpression(cronExpressionString);
                scheduledJob.setArgs(args);
                scheduledJob.setJesqueJobName(jesqueJobName);
                scheduledJob.setJesqueJobQueue(jesqueJobQueue);
                scheduledJob.setTimeZone(timeZone);

                scheduledJobDaoService.save(jedis, scheduledJob);

                //add trigger state
                Trigger trigger = new Trigger();
                trigger.setJobName(jobName);
                trigger.setNextFireTime(cronExpression.getNextValidTimeAfter(new DateTime()));
                trigger.setState(TriggerState.Waiting);
                trigger.setAcquiredBy("");

                triggerDaoService.save(jedis, trigger);

                //update trigger indexes
                String key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_NEXTFIRE_INDEX);
                jedis.zadd(key, trigger.getNextFireTime().getMillis(), jobName);

                return null;
            }
        });
    }

    public void deleteSchedule(final String name) {
        PoolUtils.doWorkInPoolNicely(this.pool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                scheduledJobDaoService.delete(jedis, name);
                triggerDaoService.delete(jedis, name);
                return null;
            }
        });
    }

    void processScheduledJobs(final DateTime until, final String hostName){
        PoolUtils.doWorkInPoolNicely(this.pool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                DateTime now = new DateTime();
                serverCheckIn(jedis, hostName, new DateTime());
                cleanUpStaleServers(jedis);
                enqueueReadyJobs(jedis, until, hostName);
                return null;
            }
        });
    }

    void enqueueReadyJobs(Jedis jedis, DateTime until, final String hostName) {
        LOG.info("enqueueReadyJobs at time " + until.toString());
        //check to see if there are any servers that have missed removing check-in
        //if so get the intersection of WATIING jobs that are not in the nextFireTimeIndex and add
        final List<Tuple> acquiredJobs = acquireJobs(jedis, until, 1, hostName);
        LOG.info("acquiredJobs size: " + acquiredJobs.size());
        if( acquiredJobs == null || acquiredJobs.size() == 0)
            return;
        long earliestAcquiredJobTime = (long)acquiredJobs.get(0).getScore();
        long delay = earliestAcquiredJobTime - DateTime.now().getMillis();
        enqueueJobs(jedis, acquiredJobs, hostName, delay);
    }

    private void enqueueJobs(Jedis jedis, List<Tuple> acquiredJobs, String hostName, long delay){
        LOG.info("enqueueJobs acquiredJobs size " + acquiredJobs.size() + ", hostName " + hostName);
        for (Tuple acquiredJob : acquiredJobs) {
            try {
                enqueueJob(jedis, acquiredJob.getElement(), hostName, delay);
            }
            catch (Exception ex){
                LOG.error("failed to enqueue job", ex);
            }
        }
    }

    private List<Tuple> acquireJobs(Jedis redis, final DateTime until, final Integer number, final String hostName) {
        String key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_NEXTFIRE_INDEX);
        Set<Tuple> allJobs = redis.zrangeWithScores(key, 0, number - 1);
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

                key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, jobName);

                redis.watch(key);
                Trigger trigger = triggerDaoService.findByJobName(redis, jobName);
                if( trigger.getState() != TriggerState.Waiting ) {
                    LOG.debug("Trigger not in waiting state for job " + jobName);
                    redis.unwatch();
                }

                trigger.setState(TriggerState.Acquired);
                trigger.setAcquiredBy(hostName);
                Transaction transaction = redis.multi();
                List<Object> transactionResult = null;
                try{
                    key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, trigger.getJobName());
                    transaction.hmset(key, trigger.toRedisHash());
                    key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_NEXTFIRE_INDEX);
                    transaction.zrem(key, jobName);
                    key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, "state", TriggerState.Acquired.name(), hostName);
                    transaction.sadd(key, jobName);
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

    private void enqueueJob(Jedis redis, String jobName, String hostName, final long delay) throws Exception {
        LOG.info("Enqueuing job " + jobName);
        String key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, jobName);
        redis.watch(key);
        ScheduledJob scheduledJob = scheduledJobDaoService.findByName(redis, jobName);
        Trigger trigger = scheduledJob.getTrigger();
        key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, "state", TriggerState.Acquired.name(), hostName);
        if(trigger.getState() != TriggerState.Acquired ) {
            LOG.warn("Trigger state is no longer " + TriggerState.Acquired.name() + " for job " + jobName);
            redis.srem(key, jobName);
            redis.unwatch();
            return;
        }
        if(!hostName.equals(trigger.getAcquiredBy())) {
            LOG.warn("Trigger state was acquired by another server " + trigger.getAcquiredBy() +" for job " + jobName);
            redis.srem(key, jobName);
            redis.unwatch();
            return;
        }

        CronExpression cronExpression = new CronExpression(scheduledJob.getCronExpression(), scheduledJob.getTimeZone());

        trigger.setNextFireTime(cronExpression.getNextValidTimeAfter(new DateTime()));
        trigger.setState(TriggerState.Waiting);
        trigger.setAcquiredBy("");

        Job job = new Job(scheduledJob.getJesqueJobName(), scheduledJob.getArgs());

        String jobJson = ObjectMapperFactory.get().writeValueAsString(job);

        Transaction transaction = redis.multi();
        try{

            key = JesqueUtils.createKey(namespace, ResqueConstants.QUEUES);
            transaction.sadd(key, scheduledJob.getJesqueJobQueue());
            key = JesqueUtils.createKey(namespace, ResqueConstants.QUEUE, scheduledJob.getJesqueJobQueue());
            transaction.rpush(key, jobJson);

            key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, trigger.getJobName());

            transaction.hmset(key, trigger.toRedisHash());

            key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_NEXTFIRE_INDEX);

            transaction.zadd(key, trigger.getNextFireTime().getMillis(), jobName);
            if( transaction.exec() == null ) {
                LOG.warn("Job exection aborted for " + jobName + " because the trigger data changed");
            }
        } catch(Exception exception) {
            LOG.error("Exception enqueuing job, discarding transaction", exception);
            transaction.discard();
        }

        //always remove index regardless of the result of the enqueue
        key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, "state", TriggerState.Acquired.name(), hostName);
        redis.srem(key, jobName);
    }


    private void serverCheckIn(Jedis jedis, String hostName, DateTime checkInDate) {
        String key = JesqueUtils.createKey(namespace, SCHEDULER_PREFIX, "checkIn");
        jedis.hset(key, hostName, String.valueOf(checkInDate.getMillis()));
    }


    private void cleanUpStaleServers(Jedis jedis) {

        DateTime now = new DateTime();
        String key = JesqueUtils.createKey(namespace, SCHEDULER_PREFIX, "checkIn");
        Map<String, String> serverCheckInHash = jedis.hgetAll(key);
        for (Map.Entry<String, String> entry : serverCheckInHash.entrySet()) {
            Seconds sec = Seconds.secondsBetween(new DateTime(Long.parseLong(entry.getValue())), now);
            if(sec.getSeconds() > STALE_SERVER_SECONDS){
                cleanUpStaleServer(jedis, entry.getKey());
            }
        }
    }

    private void cleanUpStaleServer(Jedis redis, String hostName) {
        LOG.info("Cleaning up stale server $hostName");

        String staleServerAcquiredJobsSetName = getAcquiredIndexByHostName(hostName);

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
                key = JesqueUtils.createKey(namespace, Trigger.TRIGGER_NEXTFIRE_INDEX);
                transaction.zadd(key, triggerFireTime.get(staleJobName), staleJobName);
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

      private String getAcquiredIndexByHostName(String hostName) {
        return JesqueUtils.createKey(namespace, Trigger.TRIGGER_PREFIX, "state", TriggerState.Acquired.name(), hostName);
    }



}
