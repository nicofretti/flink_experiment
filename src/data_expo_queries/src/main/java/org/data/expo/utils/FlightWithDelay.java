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
  }

  public String get_id_for_origin() {
    return String.format("%s-%s-%s", this.plane, this.datetime, this.origin);
  }

  public String get_id_for_destination() {
    return String.format("%s-%s-%s", this.plane, this.datetime, this.destination);
  }

  public String get_origin_and_dest() {
    return String.format("[%s -> %s (%d)]", this.origin, this.destination, this.delay);
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", datetime, plane, get_origin_and_dest());
  }

  public String to_csv() {
    return "";
  }
}
