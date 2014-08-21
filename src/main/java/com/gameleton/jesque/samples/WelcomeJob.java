package com.gameleton.jesque.samples;

/**
 * Created by levin on 8/15/2014.
 */
public class WelcomeJob implements Runnable {


    @Override
    public void run() {
        System.out.println("WelcomeJob");
    }
}
