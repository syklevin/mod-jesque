package com.gameleton.jesque.trigger;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by levin on 8/14/2014.
 */
public class Trigger {

    public static final String TRIGGER_PREFIX = "trigger";
    public static final String TRIGGER_NEXTFIRE_INDEX = "trigger:nextFireTime:WAITING:sorted";

    private String jobName;
    private DateTime nextFireTime;
    private TriggerState state;
    private String acquiredBy;

    public static Trigger fromRedisHash(Map<String, String> hash) {
        Trigger trigger = new Trigger();
        trigger.setJobName(hash.get("jobName"));
        trigger.setNextFireTime(new DateTime(Long.parseLong(hash.get("nextFireTime"))));
        trigger.setState(TriggerState.findByName(hash.get("state")));
        trigger.setAcquiredBy(hash.get("acquiredBy"));
        return trigger;
    }

    public Map<String, String> toRedisHash() {
        Map<String, String> map = new HashMap<>();
        map.put("jobName", getJobName());
        map.put("nextFireTime", String.valueOf(getNextFireTime().getMillis()));
        map.put("state", getState().name());
        map.put("acquiredBy", getAcquiredBy());
        return map;
    }

//    public String getRedisKey() {
//        return TRIGGER_PREFIX + ":" + getJobName();
//    }

//    public static String getRedisKeyForJobName(String jobName) {
//        return TRIGGER_PREFIX + ":" + jobName;
//    }
//
//    public static String getAcquiredIndexByHostName(String hostName) {
//        return TRIGGER_PREFIX + ":state:" + TriggerState.Acquired.name() + ":" + hostName;
//    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public DateTime getNextFireTime() {
        return nextFireTime;
    }

    public void setNextFireTime(DateTime nextFireTime) {
        this.nextFireTime = nextFireTime;
    }

    public TriggerState getState() {
        return state;
    }

    public void setState(TriggerState state) {
        this.state = state;
    }

    public String getAcquiredBy() {
        return acquiredBy;
    }

    public void setAcquiredBy(String acquiredBy) {
        this.acquiredBy = acquiredBy;
    }
}
