package org.lumi.odmatrixcalc.util;

import java.lang.Math;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 19/2/2016.
 */

public class SphericalMercator {
    public static double y2lat(double aY) {
        return Math.toDegrees(2* Math.atan(Math.exp(Math.toRadians(aY / RADIUS))) - Math.PI/2);

    }

    public static double lat2y(double aLat) {
        return Math.log(Math.tan(Math.PI / 4 + Math.toRadians(aLat) / 2)) * RADIUS;

    }

    public static double x2lon(double aX) {
        return Math.toDegrees(aX / RADIUS);

    }
    public static double lon2x(double aLong) {
        return Math.toRadians(aLong) * RADIUS;

    }

    public static final double RADIUS = 6378137.0;

}
