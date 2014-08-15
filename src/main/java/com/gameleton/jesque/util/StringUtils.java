package com.gameleton.jesque.util;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by levin on 8/15/2014.
 */
public class StringUtils {

    public static Map<String, String> getQueryMap(String query)
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String name = param.split("=")[0];
            String value = "";

            try {
                value = URLDecoder.decode(param.split("=")[1], "UTF-8");

            } catch (Exception e) {
                System.out.println("wtf exception: " + e.getMessage());
            }


            map.put(name, value);
        }

        return map;
    }
}
