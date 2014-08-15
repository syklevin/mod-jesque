package com.gameleton.jesque.trigger;

/**
 * Created by levin on 8/14/2014.
 */
public enum TriggerField {

    NextFireTime("nextFireTime"),
    State("state"),
    AcquiredBy("acquiredBy");

    String name;

    TriggerField(String name) {
        this.name = name;
    }
}
