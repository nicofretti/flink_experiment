package org.data.expo;

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
  public int arr_time;
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
    this.year = Integer.parseInt(row_split[0]);
    this.month = Integer.parseInt(row_split[1]);
    this.day_of_month = Integer.parseInt(row_split[2]);
    this.day_of_week = Integer.parseInt(row_split[3]);
    this.dep_time = Integer.parseInt(row_split[4]);
    this.crs_dep_time = Integer.parseInt(row_split[5]);
    this.arr_time = Integer.parseInt(row_split[6]);
    this.crs_arr_time = Integer.parseInt(row_split[7]);
    this.unique_carrier = row_split[8];
    this.flight_num = Integer.parseInt(row_split[9]);
    this.tail_num = row_split[10];
    this.actual_elapsed_time = Integer.parseInt(row_split[11]);
    this.crs_elapsed_time = Integer.parseInt(row_split[12]);
    this.air_time = Integer.parseInt(row_split[13]);
    this.arr_delay = Integer.parseInt(row_split[14]);
    this.dep_delay = Integer.parseInt(row_split[15]);
    this.origin = row_split[16];
    this.dest = row_split[17];
    this.distance = Integer.parseInt(row_split[18]);
    this.taxi_in = Integer.parseInt(row_split[19]);
    this.taxi_out = Integer.parseInt(row_split[20]);
    this.cancelled = Integer.parseInt(row_split[21]);
    this.cancellation_code = row_split[22];
    this.diverted = Integer.parseInt(row_split[23]);
    this.carrier_delay = Integer.parseInt(row_split[24]);
    this.weather_delay = Integer.parseInt(row_split[25]);
    this.nas_delay = Integer.parseInt(row_split[26]);
    this.security_delay = Integer.parseInt(row_split[27]);
    // this.late_aircraft_delay = Integer.parseInt(row_split[28]);
  }

  @Override
  public String toString() {
    return String.format(
        "(%d/%d/%d) %s -> %s,%d,%d", year, month, day_of_month, origin, dest, dep_delay, arr_delay);
  }
}
