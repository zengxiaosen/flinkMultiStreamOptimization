package com.z.flinkStreamOptimizatiion.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;


/**
 * 单流场景下：
 * Flink中timeWindow滚动窗口边界和数据延迟问题
 * delay代表了能够容忍的时序程度
 * 水位 = 目前最大的时间戳 - delay
 */
public class TimeWindowDemo {
    
    public static void main(String[] args) throws Exception {
        // 根据event time和窗口时间大小，计算event time所属的窗口开始时间和结束时间
        // test1();

        // 参考因素：delay + windowSize, 情况一，元素在水位以下，但windows还没被触发计算，参照record 5
        // test2();

        // 参考因素：delay + windowSize, 情况二，元素在水位以下，但windows已经无法被触发计算了
        // test3();

        // 参考因素：delay + windowSize，通过增大delay，来增大失序的容忍程度，确保不丢数据
        // test4();

        // 测试 parallism
        test5();

    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    private static void test5() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new DataSourceForTest4()).name("Demo Source").setParallelism(2);

        DataStream<WordWithCount> windowCount = dataStream.flatMap(new FlatMapFunction<Tuple3<String, String, Long>, WordWithCount>() {
            @Override
            public void flatMap(Tuple3<String, String, Long> value, Collector<WordWithCount> collector) throws Exception {
                collector.collect(new WordWithCount(value.f0, 1L));
                //collector.collect(new WordWithCount(value.f1, 1L));
                //collector.collect(new WordWithCount(String.valueOf(value.f2), 1L));
            }
        }).keyBy("word")
        .sum("count");


        windowCount.print();
        env.execute("streaming word count");
    }


    /**
     * 观察 record 5 和 record 6, 它们的时间窗口如下：
     * 窗口开始时间：1000000100000 -> 2001-09-09 09:48:20.000
     * 窗口结束时间：1000000110000 -> 2001-09-09 09:48:30.000
     * 它们进来的时候水位线如下：
     * 水位线(watermark)： 1000000109900 -> 2001-09-09 09:48:29.900
     * 也就是说，它们进来的时候，watermark < windows end time
     * 这种情况下，就算数据的 eventtime < watermark，数据还是被保留下来，没有丢失。
     * @throws Exception
     */
    private static void test4() throws Exception {
        long delay = 5100L;
        int windowSize = 10;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new DataSourceForTest4()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> watermark = dataStream.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
                    private final long maxOutOfOrderness = delay;
                    private long currentMaxTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                        long timestamp = element.f2;
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println("#### 第 " + element.f1 + " 个record ####");
                        System.out.println("currentMaxTimestamp: " + currentMaxTimestamp);
                        System.out.println("水位线(watermark)： " + (currentMaxTimestamp - maxOutOfOrderness) + " -> " + format.format(currentMaxTimestamp - maxOutOfOrderness));
                        System.out.println("窗口开始时间：" +  WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000)));
                        System.out.println("窗口结束时间：" +  (WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000) + " -> " + format.format((WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000)));
                        System.out.println(element.f1 + " -> " + timestamp + " -> " + format.format(timestamp));

                        return timestamp;
                    }
                }
        );

        // 窗口函数进行处理
        DataStream<Tuple3<String, String, Long>> resStream = watermark.keyBy(0).timeWindow(Time.seconds(windowSize))
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, "[" + value1.f1 + "," + value2.f1 + "]", 1L);
                    }
                });

        resStream.print();
        env.execute("event time demo");
    }

    /**
     * 观察record 5 和 record 6，它们的窗口属性如下：
     * 窗口开始时间：1000000100000 -> 2001-09-09 09:48:20.000
     * 窗口结束时间：1000000110000 -> 2001-09-09 09:48:30.000
     * windows end time < watermark, 这个窗口已经无法被触发计算了。
     * 也就是说，这个窗口创建时，已经 windows end time < watermark，相当于第5第6条记录都丢失了。
     * @throws Exception
     */
    private static void test3() throws Exception {
        long delay = 5000L;
        int windowSize = 10;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new DataSourceForTest3()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> watermark = dataStream.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
                    private final long maxOutOfOrderness = delay;
                    private long currentMaxTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                        long timestamp = element.f2;
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println("#### 第 " + element.f1 + " 个record ####");
                        System.out.println("currentMaxTimestamp: " + currentMaxTimestamp);
                        System.out.println("水位线(watermark)： " + (currentMaxTimestamp - maxOutOfOrderness) + " -> " + format.format(currentMaxTimestamp - maxOutOfOrderness));
                        System.out.println("窗口开始时间：" +  WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000)));
                        System.out.println("窗口结束时间：" +  (WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000) + " -> " + format.format((WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000)));
                        System.out.println(element.f1 + " -> " + timestamp + " -> " + format.format(timestamp));

                        return timestamp;
                    }
                }
        );

        // 窗口函数进行处理
        DataStream<Tuple3<String, String, Long>> resStream = watermark.keyBy(0).timeWindow(Time.seconds(windowSize))
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, "[" + value1.f1 + "," + value2.f1 + "]", 1L);
                    }
                });

        resStream.print();
        env.execute("event time demo");
    }

    /**
     * 观察record 5，对于此条记录，元素在水位以下，但windows还没被触发计算
     * 到了record 6，水位线在record 5 之上，windows被触发计算
     * @throws Exception
     */
    private static void test2() throws Exception {

        long delay = 5000L;
        int windowSize = 10;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new DataSource()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> watermark = dataStream.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
                    private final long maxOutOfOrderness = delay;
                    private long currentMaxTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    /**
                     * 触发窗口运算时机：
                     * 当一条数据过来，
                     * 1）水位线 > 上一批次的记录的窗口结束时间，之前的数据要进行窗口运算
                     * 2）水位线 > 上一批次的记录的timestamp，之前的数据要进行窗口计算
                     *
                     * 关于是否丢数据：
                     * 1）如果当前数据的EventTime在WaterMark之上，也就是EventTime > WaterMark。由于数据所属窗口
                     * 的WindowEndTime，一定是大于EventTime的。这时有WindowEndTime > EventTime > WaterMark
                     * 这种情况是一定不会丢数据的。
                     * 2）如果当前数据的EventTime在WaterMark之下，也就是WaterMark > EventTime，这时要分两种情况：
                     *  2.1）如果该数据所属窗口的WindowEndTime > WaterMark，表示窗口还没被触发，例如第5个record的情况，
                     *  即WindowEndTime > WaterMark > EventTime,这种情况数据也是不会丢失的。
                     *  2.2）如果该数据所属窗口的WaterMark > WindowEndTime, 则表示窗口已经无法被触发，
                     *  即WaterMark > WindowEndTime > EventTime, 这种情况数据也就丢失了。
                     *
                     * 如果第6条record，由于watermark > windows end time ，第6条数据所属的窗口就永远不会被触发计算了。
                     * @param element
                     * @param previousElementTimestamp
                     * @return
                     */
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                        long timestamp = element.f2;
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println("#### 第 " + element.f1 + " 个record ####");
                        System.out.println("currentMaxTimestamp: " + currentMaxTimestamp);
                        System.out.println("水位线(watermark)： " + (currentMaxTimestamp - maxOutOfOrderness) + " -> " + format.format(currentMaxTimestamp - maxOutOfOrderness));
                        System.out.println("窗口开始时间：" +  WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000)));
                        System.out.println("窗口结束时间：" +  (WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000) + " -> " + format.format((WindowComputeUtil.myGetWindowStartWithOffset(timestamp, 0, windowSize * 1000) + windowSize * 1000)));
                        System.out.println(element.f1 + " -> " + timestamp + " -> " + format.format(timestamp));

                        return timestamp;
                    }
                }
        );

        // 窗口函数进行处理
        DataStream<Tuple3<String, String, Long>> resStream = watermark.keyBy(0).timeWindow(Time.seconds(windowSize))
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, "[" + value1.f1 + "," + value2.f1 + "]", 1L);
                    }
                });

        resStream.print();
        env.execute("event time demo");



    }

    private static class DataSourceForTest4 extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {
            Tuple3[] elements = new Tuple3[]{
                    Tuple3.of("a", "1", 1000000050000L),
                    Tuple3.of("a", "2", 1000000054000L),
                    Tuple3.of("a", "3", 1000000079900L),
                    Tuple3.of("a", "4", 1000000115000L),
                    Tuple3.of("b", "5", 1000000100000L),
                    Tuple3.of("b", "6", 1000000108000L)
            };

            int count = 0;
            while (running && count < elements.length) {
                ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1, (Long) elements[count].f2));
                count++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class DataSourceForTest3 extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {
            Tuple3[] elements = new Tuple3[]{
                    Tuple3.of("a", "1", 1000000050000L),
                    Tuple3.of("a", "2", 1000000054000L),
                    Tuple3.of("a", "3", 1000000079900L),
                    Tuple3.of("a", "4", 1000000120000L),
                    Tuple3.of("b", "5", 1000000100001L),
                    Tuple3.of("b", "6", 1000000109000L)
            };

            int count = 0;
            while (running && count < elements.length) {
                ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1, (Long) elements[count].f2));
                count++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {
            Tuple3[] elements = new Tuple3[]{
                    Tuple3.of("a", "1", 1000000050000L),
                    Tuple3.of("a", "2", 1000000054000L),
                    Tuple3.of("a", "3", 1000000079900L),
                    Tuple3.of("a", "4", 1000000120000L),
                    Tuple3.of("b", "5", 1000000111000L),
                    Tuple3.of("b", "6", 1000000089000L)
            };

            int count = 0;
            while (running && count < elements.length) {
                ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1, (Long) elements[count].f2));
                count++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    private static void test1() {
        // 毫秒为单位
        long windowsize = 10000L;

        // 毫秒为单位, 滚动窗口 offset = 0L
        long offset = 0L;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long a1 = 1000000050000L;
        long a2 = 1000000054000L;
        long a3 = 1000000079900L;
        long a4 = 1000000120000L;
        long b5 = 1000000111000L;
        long b6 = 1000000089000L;

        System.out.println(a1 + " -> " + format.format(a1) + "\t所属窗口的开始时间是：" +
                WindowComputeUtil.myGetWindowStartWithOffset(a1, offset, windowsize) + " -> " +
                format.format( WindowComputeUtil.myGetWindowStartWithOffset(a1, offset, windowsize)));

        System.out.println(a2 + " -> " + format.format(a2) + "\t所属窗口的起始时间是: " + WindowComputeUtil.myGetWindowStartWithOffset(a2, offset, windowsize) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(a2, offset, windowsize)));
        System.out.println(a3 + " -> " + format.format(a3) + "\t所属窗口的起始时间是: " + WindowComputeUtil.myGetWindowStartWithOffset(a3, offset, windowsize) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(a3, offset, windowsize)));
        System.out.println(a4 + " -> " + format.format(a4) + "\t所属窗口的起始时间是: " + WindowComputeUtil.myGetWindowStartWithOffset(a4, offset, windowsize) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(a4, offset, windowsize)));
        System.out.println(b5 + " -> " + format.format(b5) + "\t所属窗口的起始时间是: " + WindowComputeUtil.myGetWindowStartWithOffset(b5, offset, windowsize) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(b5, offset, windowsize)));
        System.out.println(b6 + " -> " + format.format(b6) + "\t所属窗口的起始时间是: " + WindowComputeUtil.myGetWindowStartWithOffset(b6, offset, windowsize) + " -> " + format.format(WindowComputeUtil.myGetWindowStartWithOffset(b6, offset, windowsize)));


        System.out.println("-----------------------------------------");

    }


}
