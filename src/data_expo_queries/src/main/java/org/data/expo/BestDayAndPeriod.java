package org.data.expo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// Q1: When is the best time of week to fly to minimise delays?
public class BestDayAndPeriod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // host.docker.internal
        DataStream<Tuple2<Integer, Integer>> data_stream = env.socketTextStream("localhost", 8888)
            .flatMap(
                    (FlatMapFunction<String, DataExpoRow>)(value, out) -> {
                        out.collect(new DataExpoRow(value));
                    }, Types.POJO(DataExpoRow.class))
            .map(
                    (MapFunction<DataExpoRow, Tuple2<Integer, Integer>>)(value) -> {
                        // 0: day of week
                        // 1: delay calculation
                        return new Tuple2<>(value.day_of_week, value.actual_elapsed_time - value.crs_elapsed_time);
                    }, Types.TUPLE(Types.INT, Types.INT)
            )
            .keyBy(value -> value.f0) // Assign partitions by day of week
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .sum(1);
        data_stream.print();

        env.execute("BestDayAndPeriod");
    }

}
