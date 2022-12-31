package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.data.expo.utils.DataExpoRow;
import org.data.expo.utils.FlightDelayAccumulator;
import org.data.expo.utils.FlightWithDelay;

import java.time.Duration;
import java.time.Instant;

import static org.data.expo.utils.DataExpoMethods.get_data_stream;
import static org.data.expo.utils.DataExpoMethods.get_environment;

// Q4: Can you detect cascading failures as delays in one airport create delays in others?
public class CascadingDelays {

  static boolean DEBUG = false;

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
                        new Tuple4<Integer, Integer, Integer, Integer>(
                            value.year, value.month, value.day_of_month, value.dep_time),
                        value.origin,
                        value.dest,
                        value.actual_elapsed_time - value.crs_elapsed_time));
    // Union of the stream to merge the cascading delays
    data_stream_clean
        .keyBy(value -> value.plane)
        .window(TumblingEventTimeWindows.of(Time.seconds(2)))
        .aggregate(
            new AggregateFunction<
                FlightWithDelay, FlightDelayAccumulator, FlightDelayAccumulator>() {
              @Override
              public FlightDelayAccumulator createAccumulator() {
                return new FlightDelayAccumulator();
              }

              @Override
              public FlightDelayAccumulator add(
                  FlightWithDelay value, FlightDelayAccumulator accumulator) {
                accumulator.add_flight(value);
                return accumulator;
              }

              @Override
              public FlightDelayAccumulator getResult(FlightDelayAccumulator accumulator) {
                return accumulator;
              }

              @Override
              public FlightDelayAccumulator merge(
                  FlightDelayAccumulator a, FlightDelayAccumulator b) {
                for (FlightWithDelay f : b.get_all_flights()) {
                  a.add_flight(f);
                }
                return a;
              }
            })
        .print();
    env.execute("Q4");
  }
}
