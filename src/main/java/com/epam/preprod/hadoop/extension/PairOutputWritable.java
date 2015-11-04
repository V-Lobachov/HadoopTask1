package com.epam.preprod.hadoop.extension;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/4/2015.
 */
public class PairOutputWritable implements Writable {

    private DoubleWritable avg;
    private LongWritable sum;

    public PairOutputWritable() {
        avg = new DoubleWritable();
        sum = new LongWritable();
    }

    public PairOutputWritable(DoubleWritable avg, LongWritable sum) {
        this.avg = avg;
        this.sum = sum;
    }

    public DoubleWritable getAvg() {
        return avg;
    }

    public LongWritable getSum() {
        return sum;
    }

    public void setAvg(DoubleWritable avg) {
        this.avg = avg;
    }

    public void setSum(LongWritable sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public String toString() {
        return String.format("%.2f,%d", avg.get(), sum.get());
    }
}
