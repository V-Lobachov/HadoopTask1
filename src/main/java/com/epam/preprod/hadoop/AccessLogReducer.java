package com.epam.preprod.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogReducer extends Reducer<Text, LongWritable, NullWritable, Text> {

    private long sum;
    private double avg;
    private long counter;
    private Text output = new Text();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        initialize();
         for (LongWritable data : values){
             sum+= data.get();
             counter++;
         }
        avg = sum/counter;
        output.set(String.format("%s, %f, %d", key.toString(), avg, sum));
        context.write(NullWritable.get(), output);
    }

    private void initialize(){
        output.clear();
        counter = 0;
        sum = 0;
        avg = 0;
    }
}
