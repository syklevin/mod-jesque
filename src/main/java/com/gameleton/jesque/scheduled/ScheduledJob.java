package com.gameleton.jesque.scheduled;

import com.gameleton.jesque.trigger.Trigger;
import org.joda.time.DateTimeZone;
import org.vertx.java.core.json.JsonArray;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by levin on 8/14/2014.
 */
public class ScheduledJob {
    public static final String REDIS_PREFIX = "job";
    public static final String JOB_INDEX = "job:all";

    private String name;
    private String cronExpression;
    private DateTimeZone timeZone;
    private List args;
    private String jesqueJobName;
    private String jesqueJobQueue;
    private Trigger trigger;

    public static ScheduledJob fromRedisHash(Map<String, String> hash) {
        ScheduledJob job = new ScheduledJob();
        job.setCronExpression(hash.get("cronExpression"));
        job.setArgs(new JsonArray(hash.get("args")).toList());
        job.setJesqueJobName(hash.get("jesqueJobName"));
        job.setJesqueJobQueue(hash.get("jesqueJobQueue"));
        job.setName(hash.get("name"));
        job.setTimeZone(DateTimeZone.forID(hash.get("timeZone")));
        return job;
    }

    public static String getRedisKeyForName(String name) {
        return REDIS_PREFIX + ":" + name;
    }

    public Map<String, String> toRedisHash() {
        JsonArray argsJson = new JsonArray(getArgs());
        Map<String, String> map = new HashMap<>();
        map.put("name", getName());
        map.put("cronExpression", getCronExpression());
        map.put("args", argsJson.toString());
        map.put("jesqueJobName", getJesqueJobName());
        map.put("jesqueJobQueue", getJesqueJobQueue());
        map.put("timeZone", getTimeZone().getID());
        return map;
    }

    public String getRedisKey() {
        return REDIS_PREFIX + ":" + getName();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(DateTimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public List getArgs() {
        return args;
    }

    public void setArgs(List args) {
        this.args = args;
    }

    public String getJesqueJobName() {
        return jesqueJobName;
    }

    public void setJesqueJobName(String jesqueJobName) {
        this.jesqueJobName = jesqueJobName;
    }

    public String getJesqueJobQueue() {
        return jesqueJobQueue;
    }

    public void setJesqueJobQueue(String jesqueJobQueue) {
        this.jesqueJobQueue = jesqueJobQueue;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }
}
