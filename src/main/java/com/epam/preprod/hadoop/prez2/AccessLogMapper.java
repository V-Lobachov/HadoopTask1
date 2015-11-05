package com.epam.preprod.hadoop.prez2;


import com.epam.preprod.hadoop.extension.PairWritable;
import com.epam.preprod.hadoop.extension.WATCHER;
import com.epam.preprod.model.AccessLog;
import com.epam.preprod.utility.AccessLogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, PairWritable> {

    private HashMap<String, Integer> browserCounter = new HashMap<>();
    private static final Logger LOGGER = Logger.getLogger(AccessLogParser.class);
    private Text outputKey = new Text();
    private LongWritable requestSize = new LongWritable();
    private PairWritable outputValue = new PairWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            AccessLog parsedLog = AccessLogParser.parse(value.toString());
            browserCount(parsedLog);


            outputKey.set(parsedLog.getIp());
            requestSize.set(parsedLog.getRequestBytes());

            outputValue.setFirst(requestSize);
            outputValue.setSecond(new LongWritable(1));

            context.write(outputKey, outputValue);
        } catch (DataFormatException e) {
            context.getCounter(WATCHER.CORRUPTED).increment(1);
            LOGGER.error("Input data was corrupted, please check your data");
        }
    }

    private void browserCount(AccessLog parsedLog) {
        String key = String.format("%s %s", parsedLog.getIp(), parsedLog.getBrowser());
        Integer browserUsers = browserCounter.get(key);

        if (browserUsers != null) {
            browserCounter.put(key, browserUsers + 1);
        } else {
            browserCounter.put(key, 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> data : browserCounter.entrySet()) {
            context.getCounter("USER BROWSER", data.getKey().split(" ")[1]).increment(data.getValue());
        }

    }
}
