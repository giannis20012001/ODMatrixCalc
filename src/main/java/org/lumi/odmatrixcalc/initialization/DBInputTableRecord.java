package org.lumi.odmatrixcalc.initialization;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 17/1/2016.
 */

public class DBInputTableRecord implements DBWritable {

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setLong(1, getObjId());
        ps.setLong(2, getTrajId());
        ps.setLong(3, getT());
        ps.setDouble(4, getLon());
        ps.setDouble(5, getLat());
        ps.setDouble(6, getX());
        ps.setDouble(7, getY());

    }


    @Override
    public void readFields(ResultSet rs) throws SQLException {
        setObjId(rs.getLong("objId"));
        setTrajId(rs.getLong("trajId"));
        setT(rs.getLong("t"));
        setLon(rs.getDouble("lon"));
        setLat(rs.getDouble("lat"));
        setX(rs.getDouble("x"));
        setY(rs.getDouble("y"));

    }

    /*Constructors*/
    public DBInputTableRecord() {
        //

    }

    public DBInputTableRecord(long objId, long trajId, long t, double lon, double lat, double x, double y) {
        setObjId(objId);
        setTrajId(trajId);
        setT(t);
        setLon(lon);
        setLat(lat);
        setX(x);
        setY(y);

    }

    /*Getter & Setters*/
    public double getLon() {
        return lon;

    }

    public void setLon(double lon) {
        this.lon = lon;

    }

    public long getObjId() {
        return objId;

    }

    public void setObjId(long objId) {
        this.objId = objId;

    }

    public long getTrajId() {
        return trajId;

    }

    public void setTrajId(long trajId) {
        this.trajId = trajId;

    }

    public long getT() {
        return t;

    }

    public void setT(long t) {
        this.t = t;

    }

    public double getLat() {
        return lat;

    }

    public void setLat(double lat) {
        this.lat = lat;

    }

    public double getX() {
        return x;

    }

    public void setX(double x) {
        this.x = x;

    }

    public double getY() {
        return y;

    }

    public void setY(double y) {
        this.y = y;

    }

    private long objId;
    private long trajId;
    private long t;
    private double lon;
    private double lat;
    private double x;
    private double y;

}
