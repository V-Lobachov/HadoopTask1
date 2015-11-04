package com.epam.preprod.hadoop.io;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/4/2015.
 */
public class PairWritable implements Writable {

    private LongWritable first;
    private LongWritable second;

    public PairWritable() {
        first = new LongWritable();
        second = new LongWritable();
    }

    public PairWritable(LongWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public LongWritable getFirst() {
        return first;
    }

    public void setFirst(LongWritable first) {
        this.first = first;
    }

    public LongWritable getSecond() {
        return second;
    }

    public void setSecond(LongWritable second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return String.format("%s, %s", first, second);
    }

    @Override
    public boolean equals(Object obj) {
        PairWritable data = (PairWritable) obj;
        return this.first.equals(data.getFirst()) && this.second.equals(data.getSecond());
    }
}

