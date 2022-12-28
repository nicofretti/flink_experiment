package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.Objects;

import static org.data.expo.utils.DataExpoMethods.get_data_stream;
import static org.data.expo.utils.DataExpoMethods.get_environment;

// Q3: How does the number of people flying between different locations change over time?
public class PeopleFlyingBetweenLocations {
  static boolean DEBUG = false;

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = get_environment(DEBUG);
    // Add the watermarking
    DataStream<String> data_stream = get_data_stream(env, DEBUG);

    SingleOutputStreamOperator<Tuple3<String, String, Integer>> data_stream_clean =
        data_stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli()))
            .flatMap(
                (value, out) -> out.collect(new DataExpoRow(value)), Types.POJO(DataExpoRow.class))
            .filter(
                v ->
                    !Objects.equals(v.origin_state_airport, "0")
                        && !Objects.equals(v.dest_state_airport, "0"))
            .map(
                (v) ->
                    new Tuple3<>(
                        // Origin YYYY-MM State
                        String.format("%d-%d %s", v.year, v.month, v.origin_state_airport),
                        // Destination YYYY-MM State
                        String.format("%d-%d %s", v.year, v.month, v.dest_state_airport),
                        // Counter
                        1),
                Types.TUPLE(Types.STRING, Types.STRING, Types.INT));
    // Calculation flight by origin
    DataStream<Tuple2<String, Integer>> result_by_origin =
        data_stream_clean
            .keyBy(value -> value.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))
            .reduce((v1, v2) -> new Tuple3<>(v1.f0, "", v1.f2 + v2.f2))
            .map((v) -> new Tuple2<>(v.f0, v.f2), Types.TUPLE(Types.STRING, Types.INT));
    // Calculation flight by destination
    DataStream<Tuple2<String, Integer>> result_by_destination =
        data_stream_clean
            .keyBy(value -> value.f1)
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))
            .reduce((v1, v2) -> new Tuple3<>("", v1.f1, v1.f2 + v2.f2))
            .map((v) -> new Tuple2<>(v.f1, v.f2), Types.TUPLE(Types.STRING, Types.INT));
    // Merge the two streams and sink the result
    final FileSink<Tuple2<String, Integer>> sink =
        FileSink.forRowFormat(
                new Path("output"),
                (Encoder<Tuple2<String, Integer>>)
                    (element, stream) -> {
                      String[] split = element.f0.split(" ");
                      stream.write(
                          (String.format(
                                  "%s,%s,%s,%d\n",
                                  // Year
                                  split[0].split("-")[0],
                                  // Month
                                  split[0].split("-")[1],
                                  // Airport state
                                  split[1],
                                  // Occurrences
                                  element.f1)
                              .getBytes()));
                    })
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();
    result_by_origin
        .union(result_by_destination)
        .keyBy(value -> value.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(200)))
        .sum(1)
        .sinkTo(sink)
        .setParallelism(1);
    ;
    env.execute(
        "Q3: How does the number of people flying between different locations change over?");
  }
}
