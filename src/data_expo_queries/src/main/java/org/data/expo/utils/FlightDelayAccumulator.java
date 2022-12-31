package org.data.expo.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;

public class FlightDelayAccumulator {
  public Map<Tuple3<Integer, Integer, Integer>, SortedSet<FlightWithDelay>> delays;

  public FlightDelayAccumulator() {
    this.delays = new HashMap<>();
  }

  public void add_flight(FlightWithDelay f) {
    Tuple3<Integer, Integer, Integer> date =
        new Tuple3<>(f.datetime.f0, f.datetime.f1, f.datetime.f2);
    if (!this.delays.containsKey(date)) {
      SortedSet<FlightWithDelay> list = new TreeSet<>();
      list.add(f);
      this.delays.put(date, list);
      return;
    }
    this.delays.get(date).add(f);
  }

  public ArrayList<FlightWithDelay> get_all_flights() {
    ArrayList<FlightWithDelay> all_flights = new ArrayList<>();
    for (SortedSet<FlightWithDelay> flights : this.delays.values()) {
      all_flights.addAll(flights);
    }
    return all_flights;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for (Tuple3<Integer, Integer, Integer> key : this.delays.keySet()) {
      str.append(String.format("[%d %d %d: \n", key.f0, key.f1, key.f2));
      for (FlightWithDelay flightWithDelay : this.delays.get(key)) {
        str.append(String.format("\t%s\n", flightWithDelay.toString()));
      }
    }
    str.append("] ");
    return str.toString();
  }
}
