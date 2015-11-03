package com.epam.preprod.hadoop;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogMapper extends Mapper {

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {

    }
}
