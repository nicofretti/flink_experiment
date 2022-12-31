package org.data.expo.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;

public class FlightWithDelay {
  public String plane;
  public Tuple4<Integer, Integer, Integer, Integer> datetime;
  public String origin;
  public String destination;
  public int delay;
  public int time_departure;
  private ArrayList<Tuple2<String, Integer>> cascading_delays;

  @SuppressWarnings("unused")
  public FlightWithDelay() {}

  public FlightWithDelay(
      String plane,
      Tuple4<Integer, Integer, Integer, Integer> datetime,
      String origin,
      String destination,
      int delay) {
    this.plane = plane;
    this.datetime = datetime;
    this.origin = origin;
    this.destination = destination;
    this.delay = delay;
    this.cascading_delays = new ArrayList<>();
    this.cascading_delays.add(new Tuple2<>(this.origin + " -> " + this.destination, this.delay));
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

  public void add_cascading_delay(FlightWithDelay f) {
    this.cascading_delays.add(new Tuple2<>(f.origin + " -> " + f.destination, f.delay));
    this.origin = f.origin;
    this.destination = f.destination;
    this.delay += f.delay;
  }

  @Override
  public String toString() {
    return String.format("%s -> %s (%d)", this.origin, this.destination, this.delay);
  }

  public String to_csv() {
    return "";
  }
}
