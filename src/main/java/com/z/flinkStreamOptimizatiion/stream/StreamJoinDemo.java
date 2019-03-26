package com.z.flinkStreamOptimizatiion.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;

import static com.z.flinkStreamOptimizatiion.stream.WindowComputeUtil.myGetWindowStartWithOffset;

public class StreamJoinDemo {

    /**
     * 只有在一个窗口内的数据才能join
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // 双流join
        test1();




    }

    /**
     * 普通双流join处理方式：
     * 缺陷：join窗口的双流数据都是被缓存在内存中的，也就是说，如果某个key上的窗口数据太多就会导致JVM OOM。
     * 双流join的难点也正是在这里。
     * @throws Exception
     */
    private static void test1() throws Exception {
        /**
         * 当设置参数int windowSize = 10; long delay = 5000L;时
         * 输出为：
         * (a,1,hangzhou,1000000050000,1000000059000)
         * (a,2,hangzhou,1000000054000,1000000059000)
         * 原因：
         * window_end_time < watermark, 导致数据丢失了。
         */

        //毫秒为单位
        int windowSize = 10;
        long delay = 5100L;


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource = env.addSource(new StreamJoinDataSource1()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource = env.addSource(new StreamJoinDataSource2()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> leftStream = leftSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    private final long maxOutOfOrderness = delay;
                    private long currentMaxTimestamp = 0L;
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        long timestamp = element.f2;
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println("####################################");
                        System.out.println("element.f1: " + element.f1 );
                        //System.out.println("currentMaxTimestamp: " + currentMaxTimestamp);
                        System.out.println("水位线(watermark)： " + (currentMaxTimestamp - maxOutOfOrderness) + " -> " + format.format(currentMaxTimestamp - maxOutOfOrderness));
                        System.out.println("窗口开始时间：" +  myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + " -> " + format.format(myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000)));
                        System.out.println("窗口结束时间：" +  (myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000) + " -> " + format.format((myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000)));
                        System.out.println(element.f1 + " -> " + timestamp + " -> " + format.format(timestamp));
                        return timestamp;
                    }
                }
        );

        DataStream<Tuple3<String, String, Long>> rightStream = rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    private final long maxOutOfOrderness = delay;
                    private long currentMaxTimestamp = 0L;
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        long timestamp = element.f2;
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println("####################################");
                        System.out.println("element.f1: " + element.f1 );
                        //System.out.println("currentMaxTimestamp: " + currentMaxTimestamp);
                        System.out.println("水位线(watermark)： " + (currentMaxTimestamp - maxOutOfOrderness) + " -> " + format.format(currentMaxTimestamp - maxOutOfOrderness));
                        System.out.println("窗口开始时间：" +  myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + " -> " + format.format(myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000)));
                        System.out.println("窗口结束时间：" +  (myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000) + " -> " + format.format((myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000)));
                        System.out.println(element.f1 + " -> " + timestamp + " -> " + format.format(timestamp));
                        return timestamp;
                    }
                }
        );

        // join 操作
        leftStream.join(rightStream)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>>() {
                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) {
                        System.out.println("触发双流join窗口运算");
                        return new Tuple5<>(first.f0, first.f1, second.f1, first.f2, second.f2);
                    }
                }).print();


        env.execute("TimeWindowDemo");
    }

    private static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> w) throws Exception {
            return w.f0;
        }
    }


    private static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> w) throws Exception {
            return w.f0;
        }
    }
}
