package org.data.expo.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;

public class FlightDelayAccumulator {

  public String plane;
  public Map<Tuple3<Integer, Integer, Integer>, SortedSet<FlightWithDelay>> delays;

  public FlightDelayAccumulator(FlightWithDelay f) {
    this.delays = new HashMap<>();
    this.plane = f.plane;
    this.add_flight(f);
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

  // Method to check and filter if the accumulator has a cascading delay
  public boolean has_cascading_delays() {
    boolean has_cascading_delays = false;
    boolean correct_cascading_delays;
    Map<Tuple3<Integer, Integer, Integer>, SortedSet<FlightWithDelay>> real_delays =
        new HashMap<>();

    // Check if an array of delays has more than one element and if the flights are related (same
    // origin and destination)
    for (Tuple3<Integer, Integer, Integer> day : this.delays.keySet()) {
      // If there is only one flight, we skip it
      if (this.delays.get(day).size() <= 1) {
        continue;
      }
      FlightWithDelay[] flights = this.delays.get(day).toArray(new FlightWithDelay[0]);
      FlightWithDelay prev = flights[0];
      correct_cascading_delays = true;
      for (int i = 1; i < flights.length; i++) {
        FlightWithDelay curr = flights[i];
        if (!prev.destination.equals(curr.origin)) {
          correct_cascading_delays = false;
          break;
        }
        prev = curr;
      }
      if (correct_cascading_delays) {
        // All ok
        has_cascading_delays = true;
        real_delays.put(day, this.delays.get(day));
      }
    }
    this.delays = real_delays;
    return has_cascading_delays;
  }

  public String to_csv() {
    StringBuilder str = new StringBuilder();
    for (Tuple3<Integer, Integer, Integer> key : this.delays.keySet()) {
      str.append(String.format("%s,%d,%d,%d,", this.plane, key.f0, key.f1, key.f2));
      for (FlightWithDelay f : this.delays.get(key)) {
        str.append(String.format("%s %s %d ", f.origin, f.destination, f.delay));
      }
      str.append("\n");
    }
    return str.toString();
  }
}
