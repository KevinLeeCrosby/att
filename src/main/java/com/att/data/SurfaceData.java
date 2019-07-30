package com.att.data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The ISD-Lite data contain a fixed-width formatted subset of the complete Integrated Surface Data (ISD) for a select
 * number of observational elements.  The data are typically stored in a single file corresponding to the ISD data, i.e.
 * one file per station per year.  For more information on the ISD-Lite format, consult the ISD-Lite technical
 * document.
 *
 * Data Format
 *
 * Field 1: Pos 1-4, Length 4: Observation Year
 * Year of observation, rounded to nearest whole hour
 *
 * Field 2: Pos 6-7, Length 2: Observation Month
 * Month of observation, rounded to nearest whole hour
 *
 * Field 3: Pos 9-11, Length 2: Observation Day
 * Day of observation, rounded to nearest whole hour
 *
 * Field 4: Pos 12-13, Length 2: Observation Hour
 * Hour of observation, rounded to nearest whole hour
 *
 * Field 5: Pos 14-19, Length 6:  Air Temperature
 * The temperature of the air
 * UNITS:  Degrees Celsius
 * SCALING FACTOR: 10
 * MISSING VALUE: -9999
 *
 * Field 6: Pos 20-24, Length 6: Dew Point Temperature
 * The temperature to which a given parcel of air must be cooled at constant pressure and water vapor content in order
 * for saturation to occur.
 * UNITS: Degrees Celsius
 * SCALING FACTOR: 10
 * MISSING VALUE: -9999
 *
 * Field 7: Pos 26-31, Length 6: Sea Level Pressure
 * The air pressure relative to Mean Sea Level (MSL).
 * UNITS:  Hectopascals
 * SCALING FACTOR: 10
 * MISSING VALUE: -9999
 *
 * Field 8: Pos 32-37, Length 6: Wind Direction
 * The angle, measured in a clockwise direction, between true north and the direction from which the wind is blowing.
 * UNITS: Angular Degrees
 * SCALING FACTOR: 1
 * MISSING VALUE: -9999
 * NOTE:  Wind direction for calm winds is coded as 0.
 *
 *
 * Field 9: Pos 38-43, Length 6: Wind Speed Rate
 * The rate of horizontal travel of air past a fixed point.
 * UNITS: meters per second
 * SCALING FACTOR: 10
 * MISSING VALUE: -9999
 *
 * Field 10: Pos 44-49, Length 6: Sky Condition Total Coverage Code
 * The code that denotes the fraction of the total celestial dome covered by clouds or other obscuring phenomena.
 * MISSING VALUE: -9999
 * DOMAIN:
 * 0: None, SKC or CLR
 * 1: One okta - 1/10 or less but not zero
 * 2: Two oktas - 2/10 - 3/10, or FEW
 * 3: Three oktas - 4/10
 * 4: Four oktas - 5/10, or SCT
 * 5: Five oktas - 6/10
 * 6: Six oktas - 7/10 - 8/10
 * 7: Seven oktas - 9/10 or more but not 10/10, or BKN
 * 8: Eight oktas - 10/10, or OVC
 * 9: Sky obscured, or cloud amount cannot be estimated
 * 10: Partial obscuration
 * 11: Thin scattered
 * 12: Scattered
 * 13: Dark scattered
 * 14: Thin broken
 * 15: Broken
 * 16: Dark broken
 * 17: Thin overcast
 * 18: Overcast
 * 19: Dark overcast
 *
 * Field 11: Pos 50-55, Length 6: Liquid Precipitation Depth Dimension - One Hour Duration
 * The depth of liquid precipitation that is measured over a one hour accumulation period.
 * UNITS: millimeters
 * SCALING FACTOR: 10
 * MISSING VALUE: -9999
 * NOTE:  Trace precipitation is coded as -1
 *
 * Field 12: Pos 56-61, Length 6: Liquid Precipitation Depth Dimension - Six Hour Duration
 * The depth of liquid precipitation that is measured over a six hour accumulation period.
 * UNITS: millimeters
 * SCALING FACTOR: 10
 * MISSING VALUE: -9999
 * NOTE:  Trace precipitation is coded as -1
 *
 * @author Kevin Crosby
 */
public final class SurfaceData {
  private static final Map<String, SurfaceData> INSTANCES = new ConcurrentHashMap<>();

  private static final int MISSING_VALUE = -9999;

  static {
    System.setProperty("hadoop.home.dir", System.getenv("HADOOP_HOME"));
  }

  private static final SparkConf CONF = new SparkConf().setMaster("local").setAppName("SurfaceData");
  private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONF);

  private final JavaRDD<Record> data;

  /**
   * Parse the surface data from a file.
   *
   * This is set up as a multiton pattern to ensure same data is not parsed multiple times.
   *
   * @param file Data file to process.
   */
  private SurfaceData(String file) {
    JavaRDD<String> lines = CONTEXT.textFile(file);
    String identifier = getIdentifier(file);
    data = lines.map((Function<String, Record>)line -> new Record(identifier, line));
  }

  public static SurfaceData getInstance(File file) {
    String identifier = getIdentifier(file);
    INSTANCES.putIfAbsent(identifier, new SurfaceData(file.toString()));
    return INSTANCES.get(identifier);
  }

  public static SurfaceData getInstance(String key) {
    // if key is file, put data if necessary, else just return data
    String identifier = key.contains(".") ? getIdentifier(key) : key;
    INSTANCES.putIfAbsent(identifier, new SurfaceData(key));
    return INSTANCES.get(identifier);
  }

  /**
   * Get identifier, which is essentially the filename without the path or extension.
   * This is used as a key for the instances.
   */
  public static String getIdentifier(File file) {
    String filename = file.getName(); // strip path
    return removeExtension(filename);
  }

  public static String getIdentifier(String file) {
    String filename = new File(file).getName(); // strip path
    return removeExtension(filename);
  }

  private static String removeExtension(String filename) {
    return filename.contains(".") ? filename.substring(0, filename.lastIndexOf('.')) : filename; // strip extension
  }

  public JavaRDD<Record> getData() {
    return data;
  }

  public static JavaRDD<Record> getEmptyData() {
    return CONTEXT.emptyRDD();
  }

  public static void close() {
    CONTEXT.close();
  }

  /**
   * 1.	Air temperature (degrees Celsius * 10)
   * 2.	Dew point temperature (degrees Celsius * 10)
   * 3.	Sea level pressure (hectopascals)
   * 4.	Wind direction (angular degrees)
   * 5.	Wind speed (meters per second * 10)
   * 6.	Total cloud cover (coded, see format documentation)
   * 7.	One-hour accumulated liquid precipitation (millimeters)
   * 8.	Six-hour accumulated liquid precipitation (millimeters)
   */
  public static class Record implements Serializable, Comparable<Record> {
    private static final long serialVersionUID = 1L;

    public static final Comparator<Record> COMPARATOR =
        Comparator.comparing(Record::getYear)
            .thenComparing(Record::getMonth)
            .thenComparing(Record::getDay)
            .thenComparing(Record::getHour)
            .thenComparing(Record::getIdentifier);

    private static final Pattern SPLITTER = Pattern.compile("\\s+");

    private String identifier;
    private int year;
    private int month;
    private int day;
    private int hour;
    private Double airTemperature;         // Celsius
    private Double dewPointTemperature;    // Celsius
    private Double seaLevelPressure;       // hectopascals
    private Integer windDirection;         // angular degrees
    private Double windSpeed;              // meters per second
    private Integer totalCloudCover;       // total cloud cover (coded)
    private Double oneHourPrecipitation;   // millimeters
    private Double sixHourPrecipitation;   // millimeters

    private Record(String id, String row) {
      List<Integer> raw = SPLITTER.splitAsStream(row)
          .map(Integer::parseInt)
          .collect(Collectors.toList());
      identifier = id;
      year = raw.get(0);
      month = raw.get(1);
      day = raw.get(2);
      hour = raw.get(3);
      airTemperature = nullableDouble(raw.get(4));
      dewPointTemperature = nullableDouble(raw.get(5));
      seaLevelPressure = nullableDouble(raw.get(6));
      windDirection = nullableInteger(raw.get(7));
      windSpeed = nullableDouble(raw.get(8));
      totalCloudCover = nullableInteger(raw.get(9));
      oneHourPrecipitation = nullableDouble(raw.get(10));
      sixHourPrecipitation = nullableDouble(raw.get(11));
    }

    public String getIdentifier() {
      return identifier;
    }

    public int getYear() {
      return year;
    }

    public int getMonth() {
      return month;
    }

    public int getDay() {
      return day;
    }

    public int getHour() {
      return hour;
    }

    public Optional<Double> getAirTemperature() {
      return Optional.ofNullable(airTemperature);
    }

    public Optional<Double> getDewPointTemperature() {
      return Optional.ofNullable(dewPointTemperature);
    }

    public Optional<Double> getSeaLevelPressure() {
      return Optional.ofNullable(seaLevelPressure);
    }

    public Optional<Integer> getWindDirection() {
      return Optional.ofNullable(windDirection);
    }

    public Optional<Double> getWindSpeed() {
      return Optional.ofNullable(windSpeed);
    }

    public Optional<Integer> getTotalCloudCover() {
      return Optional.ofNullable(totalCloudCover);
    }

    public Optional<Double> getOneHourPrecipitation() {
      return Optional.ofNullable(oneHourPrecipitation);
    }

    public Optional<Double> getSixHourPrecipitation() {
      return Optional.ofNullable(sixHourPrecipitation);
    }

    private Integer nullableInteger(int value) {
      return value == MISSING_VALUE ? null : value;
    }

    private Double nullableDouble(int value) {
      return value == MISSING_VALUE ? null : value / 10d;
    }

    private int nullableInteger(Integer value) {
      return Optional.ofNullable(value)
          .orElse(MISSING_VALUE);
    }

    private int nullableDouble(Double value) {
      return Optional.ofNullable(value)
          .map(v -> 10 * v)
          .map(Math::round)
          .map(Long::intValue)
          .orElse(MISSING_VALUE);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.writeObject(identifier);
      oos.writeInt(year);
      oos.writeInt(month);
      oos.writeInt(day);
      oos.writeInt(hour);
      oos.writeInt(nullableDouble(airTemperature));
      oos.writeInt(nullableDouble(dewPointTemperature));
      oos.writeInt(nullableDouble(seaLevelPressure));
      oos.writeInt(nullableInteger(windDirection));
      oos.writeInt(nullableDouble(windSpeed));
      oos.writeInt(nullableInteger(totalCloudCover));
      oos.writeInt(nullableDouble(oneHourPrecipitation));
      oos.writeInt(nullableDouble(sixHourPrecipitation));
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
      identifier = (String)ois.readObject();
      year = ois.readInt();
      month = ois.readInt();
      day = ois.readInt();
      hour = ois.readInt();
      airTemperature = nullableDouble(ois.readInt());
      dewPointTemperature = nullableDouble(ois.readInt());
      seaLevelPressure = nullableDouble(ois.readInt());
      windDirection = nullableInteger(ois.readInt());
      windSpeed = nullableDouble(ois.readInt());
      totalCloudCover = nullableInteger(ois.readInt());
      oneHourPrecipitation = nullableDouble(ois.readInt());
      sixHourPrecipitation = nullableDouble(ois.readInt());
    }

    @Override
    public int compareTo(Record that) {
      return COMPARATOR.compare(this, that);
    }
  }
}
