package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.data.expo.utils.DataExpoRow;
import org.data.expo.utils.FlightWithDelay;

import java.time.Duration;
import java.time.Instant;

import static org.data.expo.utils.DataExpoMethods.get_data_stream;
import static org.data.expo.utils.DataExpoMethods.get_environment;

// Q4: Can you detect cascading failures as delays in one airport create delays in others?
public class CascadingDelays {

  static boolean DEBUG = true;

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = get_environment(DEBUG);
    // Add the watermarking
    DataStream<String> data_stream = get_data_stream(env, DEBUG);

    // Create the result stream
    SingleOutputStreamOperator<FlightWithDelay> data_stream_clean =
        data_stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli()))
            .flatMap(
                (value, out) -> {
                  out.collect(new DataExpoRow(value));
                },
                Types.POJO(DataExpoRow.class))
            // Record with delay
            .filter(
                value ->
                    !value.tail_num.equals("000000") & !value.tail_num.equals("0")
                        && value.actual_elapsed_time - value.crs_elapsed_time < 0)
            .map(
                value ->
                    new FlightWithDelay(
                        value.tail_num,
                        String.format("%s-%s-%s", value.year, value.month, value.day_of_month),
                        value.origin,
                        value.dest,
                        value.actual_elapsed_time - value.crs_elapsed_time));
    // Union of the stream to merge the cascading delays
    DataStream<Tuple2<String, String>> t1 =
        data_stream_clean
            .rebalance()
            .map(
                value -> new Tuple2<>(value.get_id_for_destination(), value.get_origin_and_dest()),
                Types.TUPLE(Types.STRING, Types.STRING));
    DataStream<Tuple2<String, String>> t2 =
        data_stream_clean
            .rebalance()
            .map(
                value -> new Tuple2<>(value.get_id_for_origin(), value.get_origin_and_dest()),
                Types.TUPLE(Types.STRING, Types.STRING));
    t1.join(t2)
        .where(value -> value.f0)
        .equalTo(value -> value.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(2)))
        .apply(
            (first, second) -> new Tuple2<>(first.f0, first.f1 + " -> " + second.f1),
            Types.TUPLE(Types.STRING, Types.STRING))
        .print();
    env.execute("Q4");
  }
}
