package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.data.expo.utils.DataExpoRow;

import java.time.Duration;
import java.time.Instant;

import static org.data.expo.utils.DataExpoMethods.get_data_stream;
import static org.data.expo.utils.DataExpoMethods.get_environment;

// Q3: How does the number of people flying between different locations change over time?
public class PeopleFlyingBetweenLocations {
  static boolean DEBUG = false;
  static boolean SHOW_RESULT = true;

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = get_environment(DEBUG);
    // Add the watermarking
    DataStream<String> data_stream = get_data_stream(env, DEBUG);

    SingleOutputStreamOperator<Tuple2<String, Integer>> data_stream_clean =
        data_stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli()))
            .flatMap(
                (value, out) -> out.collect(new DataExpoRow(value)), Types.POJO(DataExpoRow.class))
            .map(
                (v) ->
                    new Tuple2<>(
                        String.format(
                            "%d-%d-%s-%s",
                            v.year, v.month, v.country_origin_airport, v.country_dest_airport),
                        1),
                Types.TUPLE(Types.STRING, Types.INT)); // Assign partitions by day of week
    data_stream_clean.print();
    env.execute(
        "Q3: How does the number of people flying between different locations change over?");
  }
}
