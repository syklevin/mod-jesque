package com.gameleton.jesque.trigger;

/**
 * Created by levin on 8/14/2014.
 */
public enum TriggerState {
    Waiting("WAITING"),
    Acquired("ACQUIRED"),
    UNKNOWN("UNKNOWN");

    private String name;

    TriggerState(String name) {
        this.name = name;
    }

    static TriggerState findByName(String name) {
        for (TriggerState type : TriggerState.values()) {
            if (type.name().equals(name)) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
