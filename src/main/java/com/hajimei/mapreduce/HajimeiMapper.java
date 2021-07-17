package com.hajimei.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HajimeiMapper extends Mapper<LongWritable, Text, Text, HajimeiFlowBean>{
    Text k=new Text();
    HajimeiFlowBean v=new HajimeiFlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split("\t");
        String phNum=fields[1];
        long upFlow=Long.parseLong(fields[fields.length-3]);
        long downFlow=Long.parseLong(fields[fields.length-2]);

        k.set(phNum);
        v.set(upFlow,downFlow);
        context.write(k, v);
    }
}