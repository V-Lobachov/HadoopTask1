package com.epam.preprod.hadoop;


import com.epam.preprod.model.AccessLog;

import com.epam.preprod.utility.AccessLogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text outputKey = new Text();
    private LongWritable outputValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AccessLog parsedLog = AccessLogParser.parse(value.toString());

        outputKey.set(parsedLog.getIp());
        outputValue.set(parsedLog.getRequestBytes());


        context.write(outputKey, outputValue);
    }
}
