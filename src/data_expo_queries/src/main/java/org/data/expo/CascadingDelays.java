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
import org.data.expo.utils.FlightWithDelay;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

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
                FlightWithDelay, Map<String, ArrayList<FlightWithDelay>>, FlightWithDelay>() {
              @Override
              public Map<String, ArrayList<FlightWithDelay>> createAccumulator() {
                return new HashMap<>();
              }

              @Override
              public Map<String, ArrayList<FlightWithDelay>> add(
                  FlightWithDelay value, Map<String, ArrayList<FlightWithDelay>> accumulator) {
                if (accumulator.containsKey(value.origin)) {
                  // accumulator.get(value.origin).add(value);
                  // Remove the previous value and add the new one
                  ArrayList<FlightWithDelay> list = accumulator.get(value.origin);
                  list.add(value);
                  accumulator.put(value.destination, list);
                  accumulator.remove(value.origin);
                } else if (!accumulator.containsKey(value.destination)) {
                  ArrayList<FlightWithDelay> list = new ArrayList<>();
                  list.add(value);
                  accumulator.put(value.destination, list);
                }
                return accumulator;
              }

              @Override
              public FlightWithDelay getResult(
                  Map<String, ArrayList<FlightWithDelay>> accumulator) {
                ArrayList<FlightWithDelay> total = new ArrayList<>();
                for (String key : accumulator.keySet()) {
                  total.addAll(accumulator.get(key));
                }
                // Order the total by the field time_departure
                total.sort(Comparator.comparingInt(o -> o.datetime.f3));
                // Compute the final resul
                FlightWithDelay result = total.get(0);
                for (int i = 1; i < total.size(); i++) {
                  result.add_cascading_delay(total.get(i));
                }
                return result;
              }

              @Override
              public Map<String, ArrayList<FlightWithDelay>> merge(
                  Map<String, ArrayList<FlightWithDelay>> a,
                  Map<String, ArrayList<FlightWithDelay>> b) {
                for (Map.Entry<String, ArrayList<FlightWithDelay>> entry : b.entrySet()) {
                  if (!a.containsKey(entry.getKey())) {
                    a.put(entry.getKey(), entry.getValue());
                  } else {
                    a.get(entry.getKey()).addAll(entry.getValue());
                  }
                }
                return a;
              }
            })
        .print();
    env.execute("Q4");
  }
}
