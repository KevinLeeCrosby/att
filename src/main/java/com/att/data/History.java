package com.att.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integrated Surface Database Station History, May 2018
 *
 * USAF = Air Force stationName ID. May contain a letter in the first position.
 * WBAN = NCDC WBAN number
 * CTRY = FIPS country ID
 * ST = State for US stations
 * ICAO = ICAO ID
 * LAT = Latitude in thousandths of decimal degrees
 * LON = Longitude in thousandths of decimal degrees
 * ELEV = Elevation in meters
 * BEGIN = Beginning Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.
 * END = Ending Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.
 *
 * Notes:
 * - Missing stationName name, etc indicate the metadata are not currently available.
 * - The term "bogus" indicates that the stationName name, etc are not available.
 * - For a small % of the stationName entries in this list, climatic data are not available.
 *
 * @author Kevin Crosby
 */
public final class History {
  private static final String HISTORY_FILE = "isd-history.csv";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;

  private final Map<String, Station> STATION_MAP;

  private static History instance = null;

  /**
   * Parse the history data.
   *
   * This is set up as a singleton pattern to ensure history data is not parsed multiple times.
   */
  private History() {
    STATION_MAP = parse();
  }

  public static History getInstance() {
    if (instance == null) {
      synchronized (History.class) {
        if (instance == null) {
          instance = new History();
        }
      }
    }
    return instance;
  }

  public Station getStation(String identifier) {
    return STATION_MAP.get(identifier);
  }

  public Station getStation(File file) {
    return getStation(getIdentifier(file));
  }

  public Collection<Station> getStations() {
    return Collections.unmodifiableCollection(STATION_MAP.values());
  }

  /**
   * Get identifier, which is essentially the filename without the path, extension, OR year.
   * This is used as a key for the stations.
   */
  public static String getIdentifier(File file) {
    return getIdentifier(file.getName());
  }

  public static String getIdentifier(String filename) {
    return filename.substring(0, filename.lastIndexOf('-'));
  }

  private Map<String, Station> parse() {
    Map<String, Station> map = null;
    URL resource = getClass().getClassLoader().getResource(HISTORY_FILE);
    try {
      if (resource == null) {
        throw new FileNotFoundException();
      }
      Path path = Paths.get(resource.toURI());
      Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8);
      map = lines.skip(1)    // skip header
          .map(Station::new) // parse line into a station
          .collect(Collectors.toMap(Station::getIdentifier, Function.identity(), (a, b) -> b, ConcurrentHashMap::new)); // store station lookup via map
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }

    return map;
  }

  /**
   * Every line of the file is a station.
   *
   * Any getters that return Optional means that those columns may be empty, and require the user to resolve this on
   * their end.
   */
  public static final class Station {
    private final String usaf;
    private final String wban;
    private final String stationName;
    private final String country;
    private final String state;
    private final String icao;
    private final Double latitude;   // decimal degrees
    private final Double longitude;  // decimal degrees
    private final Double elevation;  // meters
    private final LocalDate begin;
    private final LocalDate end;
    private final String identifier; // unique identifier

    private Station(String line) {
      line = line.substring(1, line.length() - 1); // remove start and end quotes
      Scanner lineScan = new Scanner(line).useDelimiter("\",\"");
      usaf = lineScan.next();
      wban = lineScan.next();
      stationName = nullableString(lineScan.next());
      country = nullableString(lineScan.next());
      state = nullableString(lineScan.next());
      icao = nullableString(lineScan.next());
      latitude = nullableDouble(lineScan.next());
      longitude = nullableDouble(lineScan.next());
      elevation = nullableDouble(lineScan.next());
      begin = LocalDate.parse(lineScan.next(), DATE_FORMATTER);
      end = LocalDate.parse(lineScan.next(), DATE_FORMATTER);
      identifier = String.format("%s-%s", usaf, wban);
    }

    private String nullableString(String column) {
      return column.isEmpty() ? null : column;
    }

    private Double nullableDouble(String column) {
      return column.isEmpty() ? null : Double.parseDouble(column);
    }

    public String getUSAF() {
      return usaf;
    }

    public String getWBAN() {
      return wban;
    }

    public Optional<String> getStationName() {
      return Optional.ofNullable(stationName);
    }

    public Optional<String> getCountry() {
      return Optional.ofNullable(country);
    }

    public Optional<String> getState() {
      return Optional.ofNullable(state);
    }

    public Optional<String> getICAO() {
      return Optional.ofNullable(icao);
    }

    public Optional<Double> getLatitude() {
      return Optional.ofNullable(latitude);
    }

    public Optional<Double> getLongitude() {
      return Optional.ofNullable(longitude);
    }

    public Optional<Double> getElevation() {
      return Optional.ofNullable(elevation);
    }

    public LocalDate getBegin() {
      return begin;
    }

    public LocalDate getEnd() {
      return end;
    }

    public String getIdentifier() {
      return identifier;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(identifier).append(": ");
      getStationName().ifPresent(sb::append);
      sb.append(",");
      getCountry().ifPresent(sb::append);
      sb.append(",");
      getState().ifPresent(sb::append);
      sb.append(",");
      getICAO().ifPresent(sb::append);
      sb.append(", (");
      getLatitude().ifPresent(sb::append);
      sb.append(",");
      getLongitude().ifPresent(sb::append);
      sb.append(",");
      getElevation().ifPresent(sb::append);
      sb.append("), [")
          .append(begin.format(DATE_FORMATTER)).append("-").append(end.format(DATE_FORMATTER))
          .append("]");

      return sb.toString();
    }
  }
}
