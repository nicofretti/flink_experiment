package org.data.expo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PlansSufferDelay {

  public static void main(String[] args) throws Exception {
    // Init the environment
    StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
    // Set up the watermarking every one second by default
    env.getConfig().setAutoWatermarkInterval(1000L);
    // Set up the source
    DataStream<String> data_stream = env.fromCollection(DataExpoDebug.example);
  }
}
