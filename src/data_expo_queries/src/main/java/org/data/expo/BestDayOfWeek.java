package org.data.expo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// Q1: When is the best time of week to fly to minimise delays?
public class BestDayOfWeek {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
        //StreamExecutionEnvironment.getExecutionEnvironment();
        // Create a socket datastream
        // for docker replace localhost -> host.docker.internal
        //DataStream<String> data_stream = env.socketTextStream("localhost", 8888);
        // Just to debug
        DataStream<String> data_stream = env.fromCollection(DataExpoDebug.example);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> result = data_stream
                .flatMap(
                        (FlatMapFunction<String, DataExpoRow>) (value, out) -> {
                            out.collect(new DataExpoRow(value));
                        }, Types.POJO(DataExpoRow.class))
                .map(
                        (MapFunction<DataExpoRow, Tuple3<Integer, Integer, Integer>>) (value) -> {
                            // 0: day of week
                            // 1: delay calculation
                            // 2: counter of occurrences
                            return new Tuple3<>(value.day_of_week, value.actual_elapsed_time - value.crs_elapsed_time, 1);
                        }, Types.TUPLE(Types.INT, Types.INT, Types.INT)
                )
                .keyBy(value -> value.f0) // Assign partitions by day of week
                // Real window make the computation every 5 seconds
                // .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // Debug window
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .reduce(
                        (ReduceFunction<Tuple3<Integer, Integer, Integer>>) (i, j) -> new Tuple3<>(i.f0, i.f1 + j.f1, i.f2 + j.f2)
                );
        result.print();
        env.execute("BestDayOfWeek");
    }

}
