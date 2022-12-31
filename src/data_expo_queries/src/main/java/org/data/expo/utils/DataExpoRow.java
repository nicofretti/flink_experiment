package org.data.expo.utils;

// Class that identifies a row of the data set
public class DataExpoRow {
  // Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,
  // TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,
  // TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
  public int year;
  public int month;
  public int day_of_month;
  public int day_of_week;
  public int dep_time;
  public int crs_dep_time;
  public double arr_time;
  public int crs_arr_time;
  public String unique_carrier;
  public int flight_num;
  public String tail_num;
  public int actual_elapsed_time;
  public int crs_elapsed_time;
  public int air_time;
  public int arr_delay;
  public int dep_delay;
  public String origin;
  public String dest;
  public int distance;
  public int taxi_in;
  public int taxi_out;
  public int cancelled;
  public String cancellation_code;
  public int diverted;
  public int carrier_delay;
  public int weather_delay;
  public int nas_delay;
  public int security_delay;
  public int late_aircraft_delay;
  // Added column
  public int year_of_plane;
  public String dest_state_airport;
  public String origin_state_airport;

  @SuppressWarnings("unused")
  public DataExpoRow() {}

  public DataExpoRow(String row) {
    String[] row_split = row.split(",");
    // Replace empty values with 0
    for (int i = 0; i < row_split.length; i++) {
      if (row_split[i].equals("NA")) {
        row_split[i] = "0";
      }
    }
    this.year = (int) Double.parseDouble(row_split[0]);
    this.month = (int) Double.parseDouble(row_split[1]);
    this.day_of_month = (int) Double.parseDouble(row_split[2]);
    this.day_of_week = (int) Double.parseDouble(row_split[3]);
    this.dep_time = (int) Double.parseDouble(row_split[4]);
    this.crs_dep_time = (int) Double.parseDouble(row_split[5]);
    this.arr_time = Double.parseDouble(row_split[6]);
    this.crs_arr_time = (int) Double.parseDouble(row_split[7]);
    this.unique_carrier = row_split[8];
    this.flight_num = (int) Double.parseDouble(row_split[9]);
    this.tail_num = row_split[10];
    this.actual_elapsed_time = (int) Double.parseDouble(row_split[11]);
    this.crs_elapsed_time = (int) Double.parseDouble(row_split[12]);
    this.air_time = (int) Double.parseDouble(row_split[13]);
    this.arr_delay = (int) Double.parseDouble(row_split[14]);
    this.dep_delay = (int) Double.parseDouble(row_split[15]);
    this.origin = row_split[16];
    this.dest = row_split[17];
    this.distance = (int) Double.parseDouble(row_split[18]);
    this.taxi_in = (int) Double.parseDouble(row_split[19]);
    this.taxi_out = (int) Double.parseDouble(row_split[20]);
    this.cancelled = (int) Double.parseDouble(row_split[21]);
    this.cancellation_code = row_split[22];
    this.diverted = (int) Double.parseDouble(row_split[23]);
    this.carrier_delay = (int) Double.parseDouble(row_split[24]);
    this.weather_delay = (int) Double.parseDouble(row_split[25]);
    this.nas_delay = (int) Double.parseDouble(row_split[26]);
    this.security_delay = (int) Double.parseDouble(row_split[27]);
    this.late_aircraft_delay = (int) Double.parseDouble(row_split[28]);
    // Added column
    this.year_of_plane =
        !row_split[29].equals("None") ? (int) Double.parseDouble(row_split[29]) : 0;
    this.origin_state_airport = !row_split[30].equals("None") ? row_split[30] : "";
    this.dest_state_airport = !row_split[31].equals("None") ? row_split[31] : "";
  }

  @Override
  public String toString() {
    return String.format(
        "(%d/%d/%d) %s -> %s,%d,%d", year, month, day_of_month, origin, dest, dep_delay, arr_delay);
  }
}
