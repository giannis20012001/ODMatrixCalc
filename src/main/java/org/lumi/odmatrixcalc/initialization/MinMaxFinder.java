package org.lumi.odmatrixcalc.initialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.lumi.odmatrixcalc.util.SpatialTemporalPoint;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 19/2/2016.
 */

public class MinMaxFinder extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, DBInputTableRecord, NullWritable, MinMax> {
        private LinkedList<SpatialTemporalPoint> points = new LinkedList<>();
        private MinMax localMinMax = new MinMax(Double.MAX_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE);

        @Override
        public void map(LongWritable key, DBInputTableRecord record, Context context) throws IOException, InterruptedException {
            try {

                points.add(new SpatialTemporalPoint(record.getX(), record.getY(), record.getT()));

            }
            catch (Exception ex) {
                sLogger.error("Caught exception: ", ex);
                ex.printStackTrace();

            }

        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            for (SpatialTemporalPoint point : points) {
                if (point.getX() < localMinMax.getMinX()) {
                    localMinMax.setMinX(point.getX());

                }
                if (point.getX() > localMinMax.getMaxX()) {
                    localMinMax.setMaxX(point.getX());

                }
                if (point.getY() < localMinMax.getMinY()) {
                    localMinMax.setMinY(point.getY());

                }
                if (point.getY() > localMinMax.getMaxY()) {
                    localMinMax.setMaxY(point.getY());

                }
                if (point.getT() < localMinMax.getMinT()) {
                    localMinMax.setMinT(point.getT());

                }
                if (point.getT() > localMinMax.getMaxT()) {
                    localMinMax.setMaxT(point.getT());

                }

            }

            context.write(NullWritable.get(), localMinMax);

        }
    }

    public static class MyReducer extends Reducer<NullWritable, MinMax, NullWritable, NullWritable> {

        @Override
        public void reduce(NullWritable key, Iterable<MinMax> minMaxes, Context context) throws IOException, InterruptedException {
            try {

                for (MinMax localMinMax : minMaxes) {
                    if (localMinMax.getMinX() < totalMinMax.getMinX()) {
                        totalMinMax.setMinX(localMinMax.getMinX());

                    }

                    if (localMinMax.getMaxX() > totalMinMax.getMaxX()) {
                        totalMinMax.setMaxX(localMinMax.getMaxX());

                    }

                    if (localMinMax.getMinY() < totalMinMax.getMinY()) {
                        totalMinMax.setMinY(localMinMax.getMinY());

                    }

                    if (localMinMax.getMaxY() > totalMinMax.getMaxY()) {
                        totalMinMax.setMaxY(localMinMax.getMaxY());

                    }

                    if (localMinMax.getMinT() < totalMinMax.getMinT()) {
                        totalMinMax.setMinT(localMinMax.getMinT());

                    }

                    if (localMinMax.getMaxT() > totalMinMax.getMaxT()) {
                        totalMinMax.setMaxT(localMinMax.getMaxT());

                    }

                }

            }
            catch (Exception ex) {
                sLogger.error("Caught exception: ", ex);
                ex.printStackTrace();

            }

        }

    }

    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",   // driver class
                "jdbc:mysql://localhost:3306/testDb?autoReconnect=true&useSSL=false", // db url
                "mlk",    // user name
                "!1q2w3e!"); //password
        /*"jdbc:mysql://localhost:3306/testDb?autoReconnect=true&useSSL=false"*/

        //Do not use "final Job job = Job.getInstance(conf, "Database Creator")"
        Job job = new Job(conf, "Min Max Finder");
        job.setJarByClass(MinMaxFinder.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MinMax.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setNumReduceTasks(1);

        DBInputFormat.setInput(
                job,
                DBInputTableRecord.class,
                "input",   //input table name
                null,
                null,
                new String[] {"objId" ,"trajId", "t", "lon", "lat", "x", "y"}  // table columns
        );

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static MinMax execute(Configuration conf, String[] args) throws Exception {


        if(ToolRunner.run(conf, new MinMaxFinder(), args) == 0){
            return totalMinMax;
        }

        return null;

    }

    private static MinMax totalMinMax = new MinMax(Double.MAX_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE);
    private static final Logger sLogger = Logger.getLogger(MinMaxFinder.class);

}
