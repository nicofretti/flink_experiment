package org.data.expo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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

  static boolean DEBUG = false;

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = get_environment(DEBUG);
    // Add the watermarking
    DataStream<String> data_stream = get_data_stream(env, DEBUG);

    // Create the result stream
    KeyedStream<FlightWithDelay, String> data_stream_clean =
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
                        value.actual_elapsed_time - value.crs_elapsed_time))
            .keyBy(value -> value.plane + value.datetime);
    // Union of the stream to merge the cascading delays
    DataStream<FlightWithDelay> two_stream_merged =
        data_stream_clean
            .join(data_stream_clean)
            .where(value -> value.plane + value.datetime + value.destination)
            .equalTo(value -> value.plane + value.datetime + value.origin)
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))
            .apply(
                (first, second) -> {
                  first.add_cascading_delay(second.destination, second.delay);
                  return first;
                });
    two_stream_merged.print();
    // Create the sink
    // final FileSink<FlightWithDelay> sink =
    //     FileSink.forRowFormat(
    //             new Path("output"),
    //             (Encoder<FlightWithDelay>)
    //                 (element, stream) -> {
    //                   stream.write(element.to_csv().getBytes());
    //                 })
    //         .withRollingPolicy(OnCheckpointRollingPolicy.build())
    //         .build();
    // // Sink the result
    // two_stream_merged.sinkTo(sink).setParallelism(1);
    env.execute("Q4");
  }
}
