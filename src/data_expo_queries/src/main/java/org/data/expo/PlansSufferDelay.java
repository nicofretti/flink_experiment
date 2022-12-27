package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.data.expo.utils.DataExpoRow;

import java.time.Duration;
import java.time.Instant;

import static org.data.expo.utils.DataExpoMethods.get_data_stream;
import static org.data.expo.utils.DataExpoMethods.get_environment;

// Q2: Do older planes suffer more delays?
public class PlansSufferDelay {
  static boolean DEBUG = true;

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = get_environment(DEBUG);
    // Set up the source
    DataStream<String> data_stream = get_data_stream(env, DEBUG);
    // Create the result stream
    SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> data_stream_clean =
        data_stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli()))
            .flatMap(
                (value, out) -> {
                  out.collect(new DataExpoRow(value));
                },
                Types.POJO(DataExpoRow.class))
            .filter(value -> value.year_of_plane > 0)
            .map(
                (value) -> {
                  // 0: plane age
                  // 1: delay calculation
                  // 2: counter of occurrences
                  // Here are the approximate ages for an aircraft:
                  //    Old aircraft = 20+ years
                  //    Standard aircraft = 10-20 years
                  //    New aircraft = 10 years or less
                  return new Tuple3<>(
                      2007 - value.year_of_plane > 20 ? "Old" : "New",
                      value.actual_elapsed_time - value.crs_elapsed_time,
                      1);
                },
                Types.TUPLE(Types.STRING, Types.INT, Types.INT));
    // Setting up the window calculation
    DataStream<Tuple3<String, Integer, Integer>> result =
        data_stream_clean
            .keyBy(value -> value.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))
            .reduce((i, j) -> new Tuple3<>(i.f0, i.f1 + j.f1, i.f2 + j.f2));

    final FileSink<Tuple3<String, Integer, Integer>> sink =
        FileSink.forRowFormat(
                new Path("output"),
                (Encoder<Tuple3<String, Integer, Integer>>)
                    (element, stream) ->
                        stream.write(
                            (String.format(
                                    "%s,%.2f\n", element.f0, (double) element.f1 / element.f2)
                                .getBytes())))
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();
    // Writing the result, the parallelism is 1 to avoid multiple files
    result.rebalance().sinkTo(sink).setParallelism(1);
    env.execute("Q2: Do older planes suffer more delays?");
  }
}
