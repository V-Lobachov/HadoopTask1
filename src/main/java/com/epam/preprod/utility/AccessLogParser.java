package com.epam.preprod.utility;

import com.epam.preprod.model.AccessLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogParser {
    private AccessLogParser() {
    }

    private static AccessLog log = new AccessLog(); // trying to use flywaight
    private static ArrayList<String> splitedData = new ArrayList<String>();

    public static AccessLog parse(String data) throws DataFormatException {
        splitedData.clear();
        splitedData.addAll(Arrays.asList(data.split(" ")));

        try {
            log.setIp(splitedData.get(0));
            log.setRequestBytes(Integer.parseInt(splitedData.get(9)));
            log.setBrowser(parseBrowser(splitedData.get(11)));

        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            throw new DataFormatException();
        }
        return log;
    }


    private static String parseBrowser(String rawData) {
        String browser = null;
        Pattern pattern = Pattern.compile("^\"[A-Z][A-z1-9 -]*\\/\\d*.*");
        Matcher matcher = pattern.matcher(rawData);
        if (matcher.matches()) {
            browser = matcher.group();
            browser = browser.split("/")[0].substring(1);
        }else{
            browser = "Other";
        }

        return browser;
    }
}

