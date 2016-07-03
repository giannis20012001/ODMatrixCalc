package org.lumi.odmatrixcalc.util;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by apostolos on 3/7/2016.
 */
public class Result implements DBWritable {
    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1, jobCode.name());
        ps.setString(2, cellIdO.getId().toString());
        ps.setString(3, cellIdD.getId().toString());

        String listAsString = "";
        for (ID<Tuple<Long>> trajId : trajIds) {
            listAsString += (", " + trajId.toString());
        }

        ps.setString(4, listAsString);
        ps.setInt(5, trajIds.size());
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        //Todo
    }

    public JobCodes getJobCode() {
        return jobCode;
    }

    public void setJobCode(JobCodes jobCode) {
        this.jobCode = jobCode;
    }

    public ID<Tuple<Integer>> getCellIdO() {
        return cellIdO;
    }

    public void setCellIdO(ID<Tuple<Integer>> cellIdO) {
        this.cellIdO = cellIdO;
    }

    public ID<Tuple<Integer>> getCellIdD() {
        return cellIdD;
    }

    public void setCellIdD(ID<Tuple<Integer>> cellIdD) {
        this.cellIdD = cellIdD;
    }

    public Tuple<ID> getTrajIds() {
        return trajIds;
    }

    public void setTrajIds(Tuple<ID> trajIds) {
        this.trajIds = trajIds;
    }


    public enum JobCodes {
        START_END_JOB,
        PREVIOUS_NEXT_JOB
    }

    private JobCodes jobCode;
    private ID<Tuple<Integer>> cellIdO;
    private ID<Tuple<Integer>> cellIdD;
    private Tuple<ID> trajIds;

}
