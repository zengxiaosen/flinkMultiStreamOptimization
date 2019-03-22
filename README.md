# 基于Flink多流Join优化的研究与实现
## 1 伪代码：
Flink stream join的形式为Windows join 
```$xslt
stream.join(otherStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(<WindowAssigner>)
    .apply(<JoinFunction>)
```
## 2 TimeWindow滚动窗口边界与数据延迟问题
### 2.1 问题陈述
多流Join的思路是在同一窗口对多流进行Join，
每条流都是使用Flink的timeWindow api中的window size、delay、timestamp,计算触发窗口计算的时机，
每条流的延时数据，Flink根据window size、delay、延时数据的timestamp，判断是否丢弃，
本节通过调节windows size、delay，分析触发窗口计算的条件，以及触发延时数据丢失的条件。
### 2.2 数据所属窗口计算逻辑
Flink源码中，数据所属窗口的计算逻辑：
```$xslt
//Flink源码的窗口计算函数，该函数根据每条数据的timestamp、window size计算该条数据所属的[窗口开始时间，窗口结束时间]
public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
    return timestamp - (timestamp - offset + windowSize) % windowSize;
}
```
测试：根据event time和窗口时间大小，计算数据所属的窗口的开始时间和结束时间
```$xslt
//代码位置：
```





