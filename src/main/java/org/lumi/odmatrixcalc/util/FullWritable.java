package org.lumi.odmatrixcalc.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.Serializable;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public interface FullWritable<T> extends DBWritable, Serializable, Writable {

}
