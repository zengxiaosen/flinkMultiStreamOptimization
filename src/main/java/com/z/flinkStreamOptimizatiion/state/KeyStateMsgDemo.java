package com.z.flinkStreamOptimizatiion.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 与key相关的状态管理（以key分组进行状态管理）
 *
 * 补充：
 * 与key无关的state，就是与operator绑定的state，整个operator只对应一个state
 * 保存operator state的数据结构为ListState
 * 举例来说，Flink中的Kafka Connector，就是用来operator state，它会在每个connector实例中，保存该实例中消费
 * topic的所有(partition, offset)映射
 * 继承CheckpointedFunction, 实现snapshotState和restoreState
 *
 */

public class KeyStateMsgDemo {

    /**
     * if the count reaches 2, emit the average and clear the state
     * 所以Tuple2.of(1L, 3L), Tuple2.of(1L, 5L) 一组
     * 所以Tuple2.of(1L, 7L),Tuple2.of(1L, 4L)一组
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute("StafulOperator");
        System.out.println("**********************");

    }
}

