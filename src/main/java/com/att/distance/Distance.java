package com.att.distance;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.pow;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

/**
 * Calculate distance between two points in latitude, longitude, and elevation.
 *
 * Adapted from online code at ...
 *   https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude-what-am-i-doi
 *
 * @author Kevin Crosby
 */
public class Distance {
  private Distance() {}

  /**
   * Calculate distance between two points in latitude, longitude, and elevation.
   *
   * @param latitude1  Latitude of first coordinate.
   * @param longitude1 Longitude of first coordinate.
   * @param elevation1 Elevation of first coordinate.
   * @param latitude2  Latitude of second coordinate.
   * @param longitude2 Longitude of second coordinate.
   * @param elevation2 Elevation of second coordinate.
   * @return distance in meters
   */
  public static double distance(double latitude1, double longitude1, double elevation1,
                                double latitude2, double longitude2, double elevation2) {

    final int R = 6371; // radius of the earth in kilometers

    double latDistance = toRadians(latitude2 - latitude1);
    double lonDistance = toRadians(longitude2 - longitude1);
    double a = sin(latDistance / 2) * sin(latDistance / 2)
        + cos(toRadians(latitude1)) * cos(toRadians(latitude2))
        * sin(lonDistance / 2) * sin(lonDistance / 2);
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));
    double distance = R * c * 1000; // convert to meters

    double height = elevation1 - elevation2;

    distance = pow(distance, 2) + pow(height, 2);

    return sqrt(distance);
  }
}
