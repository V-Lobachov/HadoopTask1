package com.epam.preprod.hadoop;

import com.epam.preprod.hadoop.io.PairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogReducer extends Reducer<Text, PairWritable, Text, Text> {

    private long sum;
    private double avg;
    private long counter;

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
        initialize();
         for (PairWritable data : values){
             sum+= data.getFirst().get();
             counter += data.getSecond().get();
         }
        avg = (double) sum/counter;

        context.write(key, new Text(String.format("%.2f, %s", avg, sum)));
    }

    private void initialize(){
        counter = 0;
        sum = 0;
        avg = 0;
    }
}
