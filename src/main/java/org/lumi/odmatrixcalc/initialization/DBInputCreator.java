package org.lumi.odmatrixcalc.initialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.lumi.odmatrixcalc.util.SphericalMercator;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 10/1/2016.
 */

public class DBInputCreator extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, Text, DBInputTableRecord, NullWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                StringTokenizer fieldTokenizer = new StringTokenizer(line, ",");

                long objId = Long.valueOf(fieldTokenizer.nextToken());
                long trajId = Long.valueOf(fieldTokenizer.nextToken());
                Date t = (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse(fieldTokenizer.nextToken());
                double lon = Double.valueOf(fieldTokenizer.nextToken());

                double lat = Double.valueOf(fieldTokenizer.nextToken());
                double x = SphericalMercator.lon2x(lon);
                double y = SphericalMercator.lat2y(lat);

                context.write(new DBInputTableRecord(objId, trajId, t.getTime(), lon, lat, x, y), NullWritable.get());

            } catch (Exception ex) {
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
        Job job = new Job(conf, "Database Input Creator");
        job.setJarByClass(DBInputCreator.class);
        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(DBInputTableRecord.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(DBInputTableRecord.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        /*MR job can be defined with no reducer.
        In this case, all the mappers write their outputs under specified job output directory.
        So; there will be no sorting and no partitioning.
        Just set the number of reduces to 0.*/
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        DBOutputFormat.setOutput(
                job,
                "input",    // output table name
                new String[]{"objId", "trajId", "t", "lon", "lat", "x", "y"}   //table columns
        );

        return job.waitForCompletion(true) ? 0 : 1;

    }

    private static final Logger sLogger = Logger.getLogger(DBInputCreator.class);

}
