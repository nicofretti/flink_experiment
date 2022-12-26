package org.data.expo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Q2: Do older planes suffer more delays?
public class PlansSufferDelay {

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
    // Set up the source
    DataStream<String> data_stream = env.fromCollection(DataExpoDebug.example);
    // Create the result stream
    SingleOutputStreamOperator<DataExpoRow> data_stream_clean =
        data_stream.flatMap(
            (FlatMapFunction<String, DataExpoRow>)
                (value, out) -> {
                  out.collect(new DataExpoRow(value));
                },
            Types.POJO(DataExpoRow.class));
    // Print the result
    data_stream_clean.print();
    env.execute("Q2: Do older planes suffer more delays?");
  }
}
