package org.data.expo.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class FlightDelayAccumulator {
  public Map<Tuple3<Integer, Integer, Integer>, ArrayList<FlightWithDelay>> delays;

  public FlightDelayAccumulator() {
    this.delays = new HashMap<>();
  }

  public void add_flight(FlightWithDelay f) {
    Tuple3<Integer, Integer, Integer> date =
        new Tuple3<>(f.datetime.f0, f.datetime.f1, f.datetime.f2);
    if (!this.delays.containsKey(date)) {
      ArrayList<FlightWithDelay> list = new ArrayList<>();
      list.add(f);
      this.delays.put(date, list);
      return;
    }
    this.delays.get(date).add(f);
  }

  public ArrayList<FlightWithDelay> get_all_flights() {
    ArrayList<FlightWithDelay> all_flights = new ArrayList<>();
    for (ArrayList<FlightWithDelay> flights : this.delays.values()) {
      all_flights.addAll(flights);
    }
    return all_flights;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for (Map.Entry<Tuple3<Integer, Integer, Integer>, ArrayList<FlightWithDelay>> entry :
        this.delays.entrySet()) {
      str.append(String.format("[%s", entry.getKey()));
      // Order the flights by departure time
      ArrayList<FlightWithDelay> ordered = entry.getValue();
      ordered.sort(Comparator.comparingInt(f -> f.time_departure));
      for (FlightWithDelay f : ordered) {
        str.append(String.format(" %s", f.toString()));
      }
      str.append("] ");
    }
    return str.toString();
  }
}
