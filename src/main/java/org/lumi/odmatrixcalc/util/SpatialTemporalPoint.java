package org.lumi.odmatrixcalc.util;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SpatialTemporalPoint implements FullWritable<SpatialTemporalPoint> {

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(getX());
        out.writeDouble(getY());
        out.writeLong(getT());
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setDouble(1, getX());
        ps.setDouble(2, getY());
        ps.setLong(3, getT());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
        t = in.readLong();
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        this.x = rs.getDouble("x");
        this.y = rs.getDouble("y");
        this.t = rs.getLong("t");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpatialTemporalPoint)) return false;

        SpatialTemporalPoint that = (SpatialTemporalPoint) o;
        return this.x == that.x && this.y == that.y && this.t == that.t;
    }

    public SpatialTemporalPoint(double x, double y, long t) {
        this.x = x;
        this.y = y;
        this.t = t;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public long getT() {
        return t;
    }

    public void setT(long t) {
        this.t = t;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    private double x;

    @Override
    public String toString() {
        return "SpatialTemporalPoint{" +
                "x=" + x +
                ", y=" + y +
                ", t=" + t +
                '}';
    }

    private double y;
    private long t;
}
