package com.epam.preprod.utility;

import com.epam.preprod.model.AccessLog;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogParser {
    private AccessLogParser(){}

    private static final Logger LOGGER =  Logger.getLogger(AccessLogParser.class);

    private static AccessLog log = new AccessLog(); // trying to use flywaight
    private static ArrayList<String> splitedData = new ArrayList<String>();
    public static AccessLog parse(String data){
        splitedData.clear();
        splitedData.addAll(Arrays.asList(data.split(" ")));

        log.setIp(splitedData.get(0));

        try {
            log.setRequestBytes(Integer.parseInt(splitedData.get(9)));
        }catch (NumberFormatException e){
            LOGGER.error(String.format("CORRUPTED DATA %s", data));
        }
        return  log;
    }
}

