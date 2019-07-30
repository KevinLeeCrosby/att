package com.att;

import com.att.data.History;
import com.att.data.History.Station;
import com.att.data.SurfaceData;
import com.att.data.SurfaceData.Record;
import com.att.distance.Distance;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Software Engineer Coding Challenge
 *
 * The data for this challenge will be Integrated Surface Data "Lite" from NOAA weather stations. Please use the
 * following documents understand the available data.
 *
 * ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.txt
 * ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/isd-lite-technical-document.txt
 *
 * The actual weather station data files can be found at:
 *
 * ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/
 *
 * The data filenames correspond with the station numbers listed in the isd-history.txt file listed below. For example,
 * 716230-99999-2010.gz corresponds with USAF number 716230 (London Station) and WBAN number 99999.
 *
 * ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt
 *
 * The goal of this challenge is to show your skills while you pull out some useful information using this data. Your
 * program should NOT rely on any external packages with the exception of Spark if you wish, but NOT SparkSQL. Use as
 * much or little of the data as desired and show your design pattern and algorithm skills. The output of the program
 * can be whatever format you choose. Please make extensive use of code commenting to communicate your work.
 *
 * @author Kevin Crosby
 */
public class Hurricane {

  private final History history;

  private Hurricane() {
    history = History.getInstance();
  }

  /**
   * Get station identifiers with range of the coordinate.
   *
   * @param latitude  Latitude of coordinate
   * @param longitude Longitude of coordinate
   * @param elevation Elevation of coordinate
   * @param radius    Radius range of coordinate
   * @return Set of station identifiers.
   */
  private Set<String> getIdentifiers(double latitude, double longitude, double elevation, double radius) {
    Set<String> identifiers = new HashSet<>(); // station identifiers to consider
    for (Station station : history.getStations()) {
      if (station.getLatitude().isPresent() && station.getLongitude().isPresent() && station.getElevation().isPresent()) {
        double distance = Distance.distance(latitude, longitude, elevation,
            station.getLatitude().get(), station.getLongitude().get(), station.getElevation().get());
        if (distance <= radius) {
          identifiers.add(station.getIdentifier());
        }
      }
    }
    return identifiers;
  }

  /**
   * Filter files based on station identifiers.
   *
   * @param identifiers Set of station identifiers to consider.
   * @param file        File to test.
   * @return True if file has same station identifier.
   */
  private boolean filter(Set<String> identifiers, File file) {
    String identifier = History.getIdentifier(file);
    return identifiers.contains(identifier);
  }

  /**
   * List the files with identifiers.
   *
   * @param directory Base directory
   * @return List of files within identifiers.
   */
  private List<File> listFiles(String directory, Set<String> identifiers) {
    try {
      Path path = Paths.get(directory);
      return Files.list(path)
          .map(Path::toFile)
          .filter(x -> filter(identifiers, x))
          .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Determine wind speed statistics within list of files, ignoring missing data.
   *
   * @param files List of files to get wind speed.
   * @return Statistics of wind speed across all files.
   */
  private StatCounter getWindSpeedStatistics(List<File> files) {
    StatCounter stats = new StatCounter();
    for (File file : files) {
      JavaRDD<Record> data = SurfaceData.getInstance(file).getData();
      JavaDoubleRDD windSpeed = data
          .map((Function<Record, Double>)record -> record.getWindSpeed().orElse(null))
          .filter((Function<Double, Boolean>)Objects::nonNull)
          .mapToDouble((DoubleFunction<Double>)d -> d);
      stats.merge(windSpeed.stats());
    }
    return stats;
  }

  /**
   * Show the outliers on the screen.
   *
   * @param files Files to process.
   * @param delta Multiplier for standard deviation in threshold.
   */
  public void showOutliers(List<File> files, double delta) {
    findOutliers(files, System.out, delta);
  }

  /**
   * Write the statistics to a file.
   *
   * @param files      Data files to process.
   * @param outputFile Output file to write to.
   * @param delta Multiplier for standard deviation in threshold.
   */
  public void writeOutliers(List<File> files, File outputFile, double delta) {
    try {
      if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()) {
        System.err.format("Cannot create directory for file \"%s\"\n", outputFile);
        System.exit(1);
      }
      OutputStream os = new FileOutputStream(outputFile);
      findOutliers(files, os, delta);
      os.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Find large outliers indicative of hurricanes, tornados, typhoons, etc., and output to csv file.
   *
   * @param files Files to check.
   * @param os    OutputStream for output.
   * @param delta Multiplier for standard deviation in threshold.
   */
  public void findOutliers(List<File> files, OutputStream os, double delta) {
    StatCounter stats = getWindSpeedStatistics(files);
    double mean = stats.mean();
    double std = stats.sampleStdev();
    double threshold = mean + delta * std; // outlier threshold is delta * sigma more than mean

    PrintWriter out = new PrintWriter(os, true);
    out.println("YEAR,MONTH,DAY,HOUR,WIND SPEED*10(m/s),USAF-WBAN,STATION_NAME,COUNTRY,STATE,LATITUDE,LONGITUDE,ELEVATION(m)");

    // combine all of the records with a windspeed greater than the threshold
    JavaRDD<Record> data = SurfaceData.getEmptyData();
    for (File file : files) {
      JavaRDD<Record> filteredData = SurfaceData.getInstance(file).getData()
          .filter((Function<Record, Boolean>)record -> {
            Optional<Double> windSpeed = record.getWindSpeed();
            return windSpeed.isPresent() && windSpeed.get() > threshold;
          });
      data = data.union(filteredData);
    }

    // sort the records and output
    List<Record> records = new ArrayList<>(data.collect());
    records.sort(Record.COMPARATOR);

    for (Record record : records) {
      String stationID = History.getIdentifier(record.getIdentifier());
      Station station = history.getStation(stationID);
      out.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
          record.getYear(),
          record.getMonth(),
          record.getDay(),
          record.getHour(),
          record.getWindSpeed().map(d -> String.format("%d", Math.round(10 * d))).orElse(""),
          station.getIdentifier(),
          station.getStationName().orElse(""),
          station.getCountry().orElse(""),
          station.getState().orElse(""),
          station.getLatitude().map(d -> String.format("%+07.3f", d)).orElse(""),
          station.getLongitude().map(d -> String.format("%+08.3f", d)).orElse(""),
          station.getElevation().map(d -> String.format("%+07.1f", d)).orElse("")
      );
    }
  }

  /**
   * This will determine the data near the coordinates, determine the statistics of the wind speed, and then find the
   * outliers, otherwise known as hurricanes, tornados, typhoons, etc.
   *
   * @param directory Base directory
   * @param latitude  Latitude of coordinate
   * @param longitude Longitude of coordinate
   * @param elevation Elevation of coordinate
   * @param radius    Radius range of coordinate
   * @param delta Multiplier for standard deviation in threshold.
   */
  public void process(String directory, File outputFile, double latitude, double longitude, double elevation, double radius, double delta) {
    Set<String> identifiers = getIdentifiers(latitude, longitude, elevation, radius);
    List<File> files = listFiles(directory, identifiers);
    //showOutliers(files, delta); // this will display the results to the screen
    writeOutliers(files, outputFile, delta); // this will output the results to a file
  }

  /**
   * This will process the data from stations within the data contained in the base directory with a search radius from
   * the specified coordinates.
   *
   * The "delta" parameter is to customize the threshold, which is mean + delta * std.
   *
   * I chose Houston in 2008, since this covers Hurricane Ike, and possibly others.
   * The 2008 could be changed to 2017, since this covers Hurricane Harvey.
   *
   * The HADOOP_HOME environment variable needs to be set to use Spark.
   *
   * @param args base directory, outputFile, latitude, longitude, elevation, search radius, delta
   */
  public static void main(String[] args) {
    final String directory = args.length > 0 ? args[0] : "2008"; // directory where data is stored
    final String outputFilename = args.length > 1 ? args[1] : "./results.txt"; // output filename to store results

    // Houston coordinates from https://www.latlong.net/place/theater-district-houston-tx-usa-5019.html
    final double latitude = args.length > 2 ? Double.parseDouble(args[2]) : 29.761993;   // degrees
    final double longitude = args.length > 3 ? Double.parseDouble(args[3]) : -95.366302; // degrees
    final double elevation = args.length > 4 ? Double.parseDouble(args[4]) : 12; // meters

    final double radius = args.length > 5 ? Double.parseDouble(args[5]) : 600_000; // meters

    final double delta = args.length > 6 ? Double.parseDouble(args[6]) : 3d;

    File outputFile = new File(outputFilename);
    Hurricane hunter = new Hurricane();
    hunter.process(directory, outputFile, latitude, longitude, elevation, radius, delta);

    SurfaceData.close();
  }
}
