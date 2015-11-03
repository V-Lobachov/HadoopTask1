package com.epam.preprod.hadoop.test;

import com.epam.preprod.hadoop.AccessLogMapper;
import com.epam.preprod.hadoop.AccessLogReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogMapperTest {

    static {
        System.setProperty("hadoop.home.dir", "d:\\");
    }

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    private ReduceDriver<Text, LongWritable, NullWritable, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, NullWritable, Text> mapReduceDriver;

    private String  mInputData = "ip1 - - [24/Apr/2011:04:14:36 -0400] \"GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1\" 200 42011 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    private Text mOutputKey = new Text("ip1");
    private LongWritable mOutputValue = new LongWritable(42011);

    @Before
    public void initialize() {
        AccessLogMapper mapper = new AccessLogMapper();
        AccessLogReducer reducer = new AccessLogReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(mInputData));
        mapDriver.withOutput(mOutputKey, mOutputValue);
        mapDriver.runTest();
    }


}
