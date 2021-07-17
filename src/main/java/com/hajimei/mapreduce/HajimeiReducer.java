package com.hajimei.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HajimeiReducer extends Reducer<Text, HajimeiFlowBean, Text, HajimeiFlowBean> {

    @Override
    protected void reduce(Text key, Iterable<HajimeiFlowBean> values, Context context)
            throws IOException, InterruptedException {
        long sumUpFlow=0;
        long sumDownFlow=0;
        System.out.println(values);
        for (HajimeiFlowBean hajimeiFlowBean : values) {
            sumUpFlow+= hajimeiFlowBean.getUpFlow();
            sumDownFlow+= hajimeiFlowBean.getDownFlow();
        }
        HajimeiFlowBean v=new HajimeiFlowBean(sumUpFlow,sumDownFlow);
        context.write(key, v);
    }
}