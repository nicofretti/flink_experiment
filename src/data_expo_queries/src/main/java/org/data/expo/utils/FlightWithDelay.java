package org.data.expo.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class FlightWithDelay {
  public String plane;
  public String datetime;
  public String origin;
  public String destination;
  public int delay;
  private ArrayList<Tuple2<String, Integer>> cascading_delays;

  @SuppressWarnings("unused")
  public FlightWithDelay() {}

  public FlightWithDelay(
      String plane, String datetime, String origin, String destination, int delay) {
    this.plane = plane;
    this.datetime = datetime;
    this.origin = origin;
    this.destination = destination;
    this.delay = delay;
    this.cascading_delays = new ArrayList<>();
  }

  public void add_cascading_delay(String destination, int delay) {
    this.cascading_delays.add(new Tuple2<>(this.destination, this.delay));
    this.destination = destination;
    this.delay += delay;
  }

  @Override
  public String toString() {
    StringBuilder scaling = new StringBuilder(this.origin);
    // Concat previous scaling
    for (Tuple2<String, Integer> i : cascading_delays) {
      scaling.append(String.format(" -> %s", i.f0));
    }
    scaling.append(String.format(" -> %s", this.destination));
    return String.format("%s %s (%s): %d", datetime, plane, scaling, delay);
  }

  public String to_csv() {
    return "";
  }
}
