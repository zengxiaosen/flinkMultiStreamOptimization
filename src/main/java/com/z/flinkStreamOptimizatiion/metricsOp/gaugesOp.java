package com.z.flinkStreamOptimizatiion.metricsOp;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Gauges是一个简单的计量，一般用来统计瞬间状态的数量信息，比如系统中处于pending状态的job
 * 这里是测试Gauges，实时统计pending状态的job个数
 */
public class gaugesOp {
    /**
     * 实例化一个registry，最核心的一个模块，相当于应用程序的metrics系统的容器，维护一个Map
     */
    private static final MetricRegistry metrics = new MetricRegistry();
    private static Queue<String> queue = new LinkedBlockingDeque<String>();
    /**
     * 在控制台上打印输出
     */
    private static ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();
    public static void main(String[] args) throws InterruptedException {
        reporter.start(3, TimeUnit.SECONDS);

        //实例化一个Gauge
        Gauge<Integer> gauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return queue.size();
            }
        };

        //注册到容器中
        metrics.register(MetricRegistry.name(gaugesOp.class, "pending-job", "size"), gauge);

        //测试JMX
        JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).build();
        jmxReporter.start();

        //模拟数据
        for (int i=0; i< 20; i++) {
            queue.add("a");
            Thread.sleep(1000);
        }
    }
}
