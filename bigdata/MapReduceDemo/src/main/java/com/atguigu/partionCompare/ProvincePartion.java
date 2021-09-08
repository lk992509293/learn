package com.atguigu.partionCompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartion extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phone = text.toString();
        String pre = phone.substring(0, 3);

        int partion = -1;
        switch (pre) {
            case "136":
                partion = 0;
                break;
            case "137":
                partion = 1;
                break;
            case "138":
                partion = 2;
                break;
            case "139":
                partion = 3;
                break;
            default:
                partion = 4;
        }
        return partion;
    }
}
