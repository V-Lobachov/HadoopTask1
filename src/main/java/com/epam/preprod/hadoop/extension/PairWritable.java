package com.epam.preprod.hadoop.extension;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Volodymyr_Lobachov on 11/4/2015.
 */
public class PairWritable implements Writable {

    private long first;
    private long second;
    private int size;
    private Set<String> browthers;

    public PairWritable() {
        browthers = new HashSet<>();
    }

    public PairWritable(long first, long second) {
        this.first = first;
        this.second = second;
        browthers = new HashSet<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(first);
        dataOutput.writeLong(second);
        dataOutput.writeInt(browthers.size());

        for (String l :  browthers) {
            dataOutput.writeUTF(l);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readLong();
        second = dataInput.readLong();
        size = dataInput.readInt();

        for(int i=0; i<size; i++){
            browthers.add(dataInput.readUTF());
        }

    }

    public long getFirst() {
        return first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }



    public Set<String> getBrowthers() {
        return browthers;
    }


    public void setBrowthers(Set<String> browthers) {
        this.browthers = browthers;
    }

    public void addBrowser(String browser){
        browthers.add(browser);
    }

    public void addBrowsers(Set<String> browthers){
        this.browthers.addAll(browthers);
    }

    public void clanBrowthers(){
        this.browthers.clear();
    }

    @Override
    public String toString() {
        return String.format("%s, %s", first, second);
    }

    @Override
    public boolean equals(Object obj) {
        PairWritable data = (PairWritable) obj;
        return this.first == data.getFirst() && this.second == data.getSecond();
    }
}

