package org.lumi.odmatrixcalc.initialization;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 11/3/2016.
 */

public class MinMax implements Writable, DBWritable {
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(getMinX());
        out.writeDouble(getMinY());
        out.writeLong(getMinT());
        out.writeDouble(getMaxX());
        out.writeDouble(getMaxY());
        out.writeLong(getMaxT());

    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setDouble(1, getMinX());
        ps.setDouble(2, getMinY());
        ps.setLong(5, getMinT());
        ps.setDouble(3, getMaxX());
        ps.setDouble(4, getMaxY());
        ps.setLong(6, getMaxT());

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setMinX(in.readDouble());
        setMinY(in.readDouble());
        setMinT(in.readLong());
        setMaxX(in.readDouble());
        setMaxY(in.readDouble());
        setMaxT(in.readLong());

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        setMinX(rs.getDouble("minX"));
        setMinY(rs.getDouble("minY"));
        setMinT(rs.getLong("minT"));
        setMaxX(rs.getDouble("maxX"));
        setMaxY(rs.getDouble("maxY"));
        setMaxT(rs.getLong("maxT"));

    }

    @Override
    public String toString() {
        return "MinMax{" +
                "minX=" + minX +
                ", minY=" + minY +
                ", maxX=" + maxX +
                ", maxY=" + maxY +
                ", minT=" + minT +
                ", maxT=" + maxT +
                '}';

    }

    /*Constructors*/
    public MinMax() {
        //

    }

    public MinMax(double minX, double minY, double maxX, double maxY, long minT, long maxT) {
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
        this.minT = minT;
        this.maxT = maxT;

    }

    /*Getter & Setters*/
    public double getMinX() {
        return minX;

    }

    public void setMinX(double minX) {
        this.minX = minX;

    }

    public double getMinY() {
        return minY;

    }

    public void setMinY(double minY) {
        this.minY = minY;

    }

    public double getMaxX() {
        return maxX;

    }

    public void setMaxX(double maxX) {
        this.maxX = maxX;

    }

    public double getMaxY() {
        return maxY;

    }

    public void setMaxY(double maxY) {
        this.maxY = maxY;

    }

    public long getMinT() {
        return minT;

    }

    public void setMinT(long minT) {
        this.minT = minT;

    }

    public long getMaxT() {
        return maxT;

    }

    public void setMaxT(long maxT) {
        this.maxT = maxT;

    }

    private double minX;
    private double minY;
    private double maxX;
    private double maxY;
    private long minT;
    private long maxT;

}
