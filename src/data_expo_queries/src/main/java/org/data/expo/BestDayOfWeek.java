package org.data.expo;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;
import java.time.Instant;

// Q1: When is the best time of week to fly to minimise delays?
public class BestDayOfWeek {
  public static void main(String[] args) throws Exception {
    // Debugging:
    StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    // DataStream<String> data_stream = env.fromCollection(DataExpoDebug.example);
    // Deployment: for docker replace localhost -> host.docker.internal
    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> data_stream = env.socketTextStream("localhost", 8888);
    // Create the result stream
    SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> data_stream_clean =
        data_stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli()))
            .flatMap(
                (FlatMapFunction<String, DataExpoRow>)
                    (value, out) -> {
                      out.collect(new DataExpoRow(value));
                    },
                Types.POJO(DataExpoRow.class))
            .map(
                (MapFunction<DataExpoRow, Tuple3<Integer, Integer, Integer>>)
                    (value) -> {
                      // 0: day of week
                      // 1: delay calculation
                      // 2: counter of occurrences
                      return new Tuple3<>(
                          value.day_of_week, value.actual_elapsed_time - value.crs_elapsed_time, 1);
                    },
                Types.TUPLE(Types.INT, Types.INT, Types.INT)); // Assign partitions by day of week
    // Setting up the window calculation
    DataStream<Tuple3<Integer, Integer, Integer>> result =
        data_stream_clean
            .keyBy(value -> value.f0)
            // Deployment: every 10 seconds there is a calculation
            .window(TumblingEventTimeWindows.of(Time.seconds(4)))
            // .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
            // Debug window processed instantly
            // .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
            // .trigger(PurgingTrigger.of(TimerTrigger.of(Time.seconds(100))))
            .reduce(
                (ReduceFunction<Tuple3<Integer, Integer, Integer>>)
                    (i, j) -> new Tuple3<>(i.f0, i.f1 + j.f1, i.f2 + j.f2));
    // Creating the sink of output file
    final FileSink<Tuple3<Integer, Integer, Integer>> sink =
        FileSink.forRowFormat(
                new Path("output"),
                (Encoder<Tuple3<Integer, Integer, Integer>>)
                    (element, stream) ->
                        stream.write(
                            (String.format(
                                    "%d,%.2f\n", element.f0, ((double) element.f1 / element.f2))
                                .getBytes())))
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();
    // Writing the result, the parallelism is 1 to avoid multiple files
    Object end = result.sinkTo(sink).setParallelism(1);
    env.execute("BestDayOfWeek");
  }
  // override the ProcessingTimeTrigger behavior
  public static class TimerTrigger<W extends Window> extends Trigger<Object, W> {
    private static final long serialVersionUID = 1L;
    private final long interval;
    private final ReducingStateDescriptor<Long> stateDesc;

    private TimerTrigger(long winInterValMills) { // window
      this.stateDesc =
          new ReducingStateDescriptor("fire-time", new TimerTrigger.Min(), LongSerializer.INSTANCE);
      this.interval = winInterValMills;
    }

    public static <W extends Window> TimerTrigger<W> of(Time interval) {
      return new TimerTrigger(interval.toMilliseconds());
    }

    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
        throws Exception {
      if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
        // if the watermark is already past the window fire immediately
        return TriggerResult.FIRE;
      }
      long now = System.currentTimeMillis();
      ReducingState<Long> fireTimestamp = (ReducingState) ctx.getPartitionedState(this.stateDesc);
      if (fireTimestamp.get() == null) {
        long time = Math.max(timestamp, window.maxTimestamp()) + interval;
        if (now - window.maxTimestamp() > interval) { // fire late
          time = (now - now % 1000) + interval - 1;
        }
        ctx.registerProcessingTimeTimer(time);
        fireTimestamp.add(time);
        return TriggerResult.CONTINUE;
      } else {
        return TriggerResult.CONTINUE;
      }
    }

    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
      if (time == window.maxTimestamp()) {
        return TriggerResult.FIRE;
      }
      return TriggerResult.CONTINUE;
    }

    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
        throws Exception {
      ReducingState<Long> fireTimestamp = (ReducingState) ctx.getPartitionedState(this.stateDesc);
      if (((Long) fireTimestamp.get()).equals(time)) {
        fireTimestamp.clear();
        long maxTimestamp = Math.max(window.maxTimestamp(), time); // maybe useless
        if (maxTimestamp == time) {
          maxTimestamp = time + this.interval;
        }
        fireTimestamp.add(maxTimestamp);
        ctx.registerProcessingTimeTimer(maxTimestamp);
        return TriggerResult.FIRE;
      } else {
        return TriggerResult.CONTINUE;
      }
    }

    public void clear(W window, TriggerContext ctx) throws Exception {
      ReducingState<Long> fireTimestamp = (ReducingState) ctx.getPartitionedState(this.stateDesc);
      long timestamp = (Long) fireTimestamp.get();
      ctx.deleteProcessingTimeTimer(timestamp);
      fireTimestamp.clear();
    }

    public boolean canMerge() {
      return true;
    }

    public void onMerge(W window, OnMergeContext ctx) {
      ctx.mergePartitionedState(this.stateDesc);
    }

    @VisibleForTesting
    public long getInterval() {
      return this.interval;
    }

    public String toString() {
      return "TimerTrigger(" + this.interval + ")";
    }

    private static class Min implements ReduceFunction<Long> {
      private static final long serialVersionUID = 1L;

      private Min() {}

      public Long reduce(Long value1, Long value2) throws Exception {
        return Math.min(value1, value2);
      }
    }
  }
}
