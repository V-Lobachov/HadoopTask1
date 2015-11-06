package com.epam.preprod.hadoop.prez1;

import com.epam.preprod.hadoop.extension.PairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/4/2015.
 */
public class AccessLogCombiner extends Reducer<Text, PairWritable, Text, PairWritable> {

    private long sum;
    private long counter;
    private PairWritable value = new PairWritable();

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
        initialize();
        for (PairWritable data : values) {
            sum = sum + data.getFirst();
            counter +=data.getSecond();
        }

        value.setFirst(sum);
        value.setSecond(counter);

        context.write(key, value);
    }

    private void initialize() {
        counter = 0;
        sum = 0;
    }
}
