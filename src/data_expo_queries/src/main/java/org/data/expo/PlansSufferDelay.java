package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.Instant;

// Q2: Do older planes suffer more delays?
public class PlansSufferDelay {

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    // Set up the source
    DataStream<String> data_stream =
        env.fromCollection(DataExpoDebug.example)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli()));
    // Create the result stream
    SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> data_stream_clean =
        data_stream
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
                  return new Tuple3<>(
                      value.year_of_plane + "",
                      value.actual_elapsed_time - value.crs_elapsed_time,
                      1);
                },
                Types.TUPLE(Types.STRING, Types.INT, Types.INT));
    // Setting up the window calculation
    DataStream<Tuple2<String, Double>> result =
        data_stream_clean
            .keyBy(value -> value.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .reduce((i, j) -> new Tuple3<>(i.f0, i.f1 + j.f1, i.f2 + j.f2))
            .map(
                (value) -> new Tuple2<>(value.f0, ((double) value.f1) / value.f2),
                Types.TUPLE(Types.STRING, Types.DOUBLE));
    // Writing the result, the parallelism is 1 to avoid multiple files
    result.print();
    env.execute("Q2: Do older planes suffer more delays?");
  }
}
