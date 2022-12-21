package org.data.expo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

// Q1: When is the best time of week to fly to minimise delays?
public class BestDayOfWeek {
  public static void main(String[] args) throws Exception {
    // Debugging:
    // StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
    // DataStream<String> data_stream = env.fromCollection(DataExpoDebug.example);
    // Deployment: for docker replace localhost -> host.docker.internal
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> data_stream = env.socketTextStream("localhost", 8888);
    // Create the result stream
    SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> result =
        data_stream
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
                Types.TUPLE(Types.INT, Types.INT, Types.INT))
            .keyBy(value -> value.f0) // Assign partitions by day of week
            // Deployment: every 10 seconds there is a calculation
            //.window(TumblingEventTimeWindows.of(Time.seconds(2)))
            // Debug window processed instantly
            .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
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
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withInactivityInterval(Duration.ofSeconds(2))
                    .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                    .build())
            .build();
    // Writing the result, the parallelism is 1 to avoid multiple files
    result.sinkTo(sink).setParallelism(1);
    env.execute("BestDayOfWeek");
  }
}
