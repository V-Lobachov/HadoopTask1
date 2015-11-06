package com.epam.preprod.hadoop.test;

import com.epam.preprod.hadoop.prez1.AccessLogCombiner;
import com.epam.preprod.hadoop.extension.WATCHER;
import com.epam.preprod.hadoop.prez1.AccessLogMapper;
import com.epam.preprod.hadoop.prez1.AccessLogReducer;
import com.epam.preprod.hadoop.extension.PairWritable;
import com.epam.preprod.model.AccessLog;
import com.epam.preprod.utility.AccessLogParser;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogTest {


    private MapDriver<LongWritable, Text, Text, PairWritable> mapDriver;
    private ReduceDriver<Text, PairWritable, Text, Text> reduceDriver;
    private ReduceDriver<Text, PairWritable, Text, PairWritable> combinerDriver;
    private MapReduceDriver<LongWritable, Text, Text, PairWritable, Text, Text> mapReduceDriver;

    private String mInputData = "ip1 - - [24/Apr/2011:04:14:36 -0400] \"GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1\" 200 42011 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    private Text mOutputKey = new Text("ip1");
    private long requestBytes = 42011L;
    private PairWritable mOutputValue = new PairWritable();

    @Before
    public void initialize() {
        AccessLogMapper mapper = new AccessLogMapper();
        AccessLogReducer reducer = new AccessLogReducer();
        AccessLogCombiner combiner = new AccessLogCombiner();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        combinerDriver = ReduceDriver.newReduceDriver(combiner);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(mInputData));

        mOutputValue.setFirst(requestBytes);
        mOutputValue.setSecond(1);

        mapDriver.withOutput(mOutputKey, mOutputValue);
        mapDriver.runTest();
    }

    @Test
    public void textCorruptedMapperData() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(""));
        mapDriver.runTest();
        Assert.assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(WATCHER.CORRUPTED).getValue());
    }

    @Test
    public void testReducer() throws IOException {
        List<PairWritable> values = new ArrayList<>();
        values.add(new PairWritable(210, 1));
        values.add(new PairWritable(90, 1));

        reduceDriver.withInput(new Text("ip7"), values);

        reduceDriver.withOutput(new Text("ip7"), new Text("150.00,300"));
        reduceDriver.runTest();
    }

    @Test
    public void testCombiner() throws IOException {
        List<PairWritable> values = new ArrayList<>();
        values.add(new PairWritable(210, 1));
        values.add(new PairWritable(90, 1));

        combinerDriver.withInput(new Text("ip7"), values);

        combinerDriver.withOutput(new Text("ip7"), new PairWritable(300, 2));
        combinerDriver.runTest();
    }


    @Test
    public void testBrowserParser() throws Exception {
        String input = "ip44 - - [24/Apr/2011:06:38:33 -0400] \"GET / HTTP/1.1\" 200 12550 \"-\" \"Mozilla/5.0 (compatible; YodaoBot/1.0; http://www.yodao.com/help/webmaster/spider/; )\"";
        AccessLog parsedLog = AccessLogParser.parse(input);
        Assert.assertEquals("Expected BROWSER", "Mozilla", parsedLog.getBrowser());

    }


}
