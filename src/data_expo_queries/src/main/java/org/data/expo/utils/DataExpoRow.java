package org.data.expo.utils;

// Class that identifies a row of the data set
public class DataExpoRow {
  public int year;
  public int month;
  public int day_of_month;
  public int day_of_week;
  public int dep_time;
  public String tail_num;
  public int actual_elapsed_time;
  public int crs_elapsed_time;
  public String origin;
  public String dest;
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
    this.tail_num = row_split[5];
    this.actual_elapsed_time = (int) Double.parseDouble(row_split[6]);
    this.crs_elapsed_time = (int) Double.parseDouble(row_split[7]);
    this.origin = row_split[8];
    this.dest = row_split[9];
    // Added column
    this.year_of_plane =
        !row_split[10].equals("None") ? (int) Double.parseDouble(row_split[10]) : 0;
    this.origin_state_airport = !row_split[11].equals("None") ? row_split[11] : "";
    this.dest_state_airport = !row_split[12].equals("None") ? row_split[12] : "";
  }

  @Override
  public String toString() {
    return String.format(
        "DataExpoRow: %s (%d/%d/%d) %s -> %s", tail_num, year, month, day_of_month, origin, dest);
  }
}
