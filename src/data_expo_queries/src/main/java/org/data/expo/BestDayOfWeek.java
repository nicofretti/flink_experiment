package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
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

import java.time.Duration;
import java.time.Instant;

// Q1: When is the best day of the week to fly to minimise delays?
public class BestDayOfWeek {

  static boolean LOCAL_ENV = true;
  static boolean DEBUG = false;
  static boolean SHOW_RESULT = true;

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env;
    if (LOCAL_ENV) {
      env = LocalStreamEnvironment.createLocalEnvironment();
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    // Set up the watermarking every one second by default
    env.getConfig().setAutoWatermarkInterval(1000L);
    // Set up the source
    DataStream<String> data_stream;
    if (DEBUG) {
      data_stream = env.fromCollection(DataExpoDebug.example);
    } else {
      data_stream = env.socketTextStream("localhost", 8888);
    }
    // By default, the events are closed in a window of 2 seconds, but if we want the final result
    // we have to set the window larger as much as the streaming time (in this case 1 hour)
    int process_time = 2; // Seconds of window
    if (SHOW_RESULT) {
      process_time = 3600;
    }
    // Create the result stream
    SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> data_stream_clean =
        data_stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
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
            .window(TumblingEventTimeWindows.of(Time.seconds(process_time)))
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
    result.sinkTo(sink).setParallelism(1);
    env.execute("BestDayOfWeek");
  }
}
