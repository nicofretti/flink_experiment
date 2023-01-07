package org.data.expo.utils;

import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Objects;

public class FlightWithDelay implements Comparable<FlightWithDelay> {
  public String plane;
  public Tuple4<Integer, Integer, Integer, Integer> datetime;
  public String origin;
  public String destination;
  public int delay;

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
    return String.format(
        "%s (%d, %d, %d, %d): %s -> %s (%d)",
        this.plane,
        this.datetime.f0,
        this.datetime.f1,
        this.datetime.f2,
        this.datetime.f3,
        this.origin,
        this.destination,
        this.delay);
  }

  // Method compareTo is used to sort the flights by date
  public int compareTo(FlightWithDelay f) {
    // By year
    if (!Objects.equals(this.datetime.f0, f.datetime.f0)) {
      return this.datetime.f0 - f.datetime.f0;
    }
    // By month
    if (!Objects.equals(this.datetime.f1, f.datetime.f1)) {
      return this.datetime.f1 - f.datetime.f1;
    }
    // By day
    if (!Objects.equals(this.datetime.f2, f.datetime.f2)) {
      return this.datetime.f2 - f.datetime.f2;
    }
    // By hour
    return this.datetime.f3 - f.datetime.f3;
  }
}
