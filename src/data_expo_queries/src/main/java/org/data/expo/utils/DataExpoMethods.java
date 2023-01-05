package org.data.expo.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Useful methods to be used in the other classes to avoid code duplication
public class DataExpoMethods {
  public static StreamExecutionEnvironment get_environment(Boolean debug) {
    // Init the environment
    StreamExecutionEnvironment env =
        debug
            ? LocalStreamEnvironment.createLocalEnvironment()
            : StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    return env;
  }

  public static DataStream<String> get_data_stream(StreamExecutionEnvironment env, Boolean debug) {
    return debug
        ? env.fromCollection(DataExpoDebug.example)
        : env.socketTextStream("localhost", 8888);
  }
}
