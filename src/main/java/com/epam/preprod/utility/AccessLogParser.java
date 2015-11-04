package com.epam.preprod.utility;

import com.epam.preprod.model.AccessLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.DataFormatException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogParser {
    private AccessLogParser(){}

    private static AccessLog log = new AccessLog(); // trying to use flywaight
    private static ArrayList<String> splitedData = new ArrayList<String>();
    public static AccessLog parse(String data) throws DataFormatException {
        splitedData.clear();
        splitedData.addAll(Arrays.asList(data.split(" ")));

        log.setIp(splitedData.get(0));

        try {
            log.setRequestBytes(Integer.parseInt(splitedData.get(9)));
        }catch (NumberFormatException |IndexOutOfBoundsException e  ){
            throw new DataFormatException();
        }
        return  log;
    }
}

