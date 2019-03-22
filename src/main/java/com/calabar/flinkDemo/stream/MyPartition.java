package com.calabar.flinkDemo.stream;


import org.apache.flink.api.common.functions.Partitioner;

public class MyPartition  implements Partitioner<Long> {


    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数："+numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }


}
