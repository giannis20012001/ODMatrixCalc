package org.lumi.odmatrixcalc.celloriented;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.lumi.odmatrixcalc.initialization.DBInputTableRecord;

import org.lumi.odmatrixcalc.util.ID;
import org.lumi.odmatrixcalc.util.SerializableWrapper;
import org.lumi.odmatrixcalc.util.SpatialTemporalGrid;
import org.lumi.odmatrixcalc.util.SpatialTemporalPoint;
import org.lumi.odmatrixcalc.util.Tuple;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public class FirstMapReduce extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, DBInputTableRecord, ID, Tuple> {
        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            grid = new SpatialTemporalGrid(
                    Integer.valueOf(conf.get("numberOfCells")),
                    Double.valueOf(conf.get("minX")),
                    Double.valueOf(conf.get("maxX")),
                    Double.valueOf(conf.get("minY")),
                    Double.valueOf(conf.get("maxY")),
                    Long.valueOf(conf.get("minT")),
                    Long.valueOf(conf.get("maxT"))
            );

            FirstMRStartTime = System.currentTimeMillis();

        }

        @Override
        public void map(final LongWritable key, final DBInputTableRecord record, final Mapper.Context context) throws IOException, InterruptedException {
            SpatialTemporalPoint point = new SpatialTemporalPoint(record.getX(), record.getY(), record.getT());
            Tuple<Long> trajIdAsTuple = new Tuple(2);
            trajIdAsTuple.add(record.getObjId());
            trajIdAsTuple.add(record.getTrajId());

            Tuple<SerializableWrapper> traIdAndPoint = new Tuple<>(2);
            traIdAndPoint.add(new SerializableWrapper<>(new ID<>(trajIdAsTuple)));
            traIdAndPoint.add(new SerializableWrapper<>(point));

            context.write(grid.getCellId(point), traIdAndPoint);

        }

        @Override
        public void cleanup(Mapper.Context context) throws IOException, InterruptedException{
            FirstMRElapsedTimeInSec = (System.currentTimeMillis() - FirstMRStartTime);
            System.out.println("Map() took " + FirstMRElapsedTimeInSec + " milliseconds.");

        }

        private SpatialTemporalGrid grid;

    }

    public static class MyReducer extends Reducer<ID, Tuple, ID, Tuple> {
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            FirstMRStartTime = System.currentTimeMillis();

        }

        @Override
        public void reduce(ID cellId, Iterable<Tuple> tuples, Context context) throws IOException, InterruptedException {
            Hashtable<Tuple, Tuple> map = new Hashtable<>();

            for (Tuple<SerializableWrapper> tuple : tuples) {
                Tuple<ID> cellAndTrajId = new Tuple<>(2);
                cellAndTrajId.add(cellId);
                cellAndTrajId.add((ID) tuple.get(0).getObject());

                SpatialTemporalPoint point = (SpatialTemporalPoint) tuple.get(1).getObject();

                Tuple<SpatialTemporalPoint> points = map.get(cellAndTrajId);
                if (points == null) {
                    Tuple<SpatialTemporalPoint> list = new Tuple<>();
                    list.add(point);
                    map.put(cellAndTrajId, list);

                } else {
                    points.add(point);
                }
            }

            Set entrySet = map.entrySet();
            Iterator it = entrySet.iterator();

            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                Tuple<ID> cellAndTrajId = (Tuple<ID>) entry.getKey();
                Tuple<SpatialTemporalPoint> points = (Tuple<SpatialTemporalPoint>) entry.getValue();

                Tuple<SerializableWrapper> traIdAndPoints = new Tuple<>(2);
                traIdAndPoints.add(new SerializableWrapper<>(cellAndTrajId.get(1)));
                traIdAndPoints.add(new SerializableWrapper<>(points));


                /*Debugging Code*/
                /*System.err.println("<-------------START------------->");
                System.err.println("CellId, TrajID: " + cellId + "," + cellAndTrajId.get(1).getId());
                for (SpatialTemporalPoint point : points) {
                    System.err.println(point);
                }
                System.err.println("<--------------END-------------->");
                System.err.println();*/
                /****************/

                context.write(cellId, traIdAndPoints);

            }

        }

        @Override
        public void cleanup(Reducer.Context context) throws IOException, InterruptedException{
            FirstMRElapsedTimeInSec = (System.currentTimeMillis() - FirstMRStartTime);
            System.out.println("Reduce() took " + FirstMRElapsedTimeInSec + " milliseconds.");

        }

    }

    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("minX", args[0]);
        conf.set("maxX", args[1]);
        conf.set("minY", args[2]);
        conf.set("maxY", args[3]);
        conf.set("minT", args[4]);
        conf.set("maxT", args[5]);
        conf.set("numberOfCells", args[6]);

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",   // driver class
                "jdbc:mysql://192.168.100.100:3306/testDb?autoReconnect=true&useSSL=false", // db url
                "mlk",    // user name
                "!1q2w3e!"); //password

        //Do not use "final Job job = Job.getInstance(conf, "Database Creator")"
        Job job = new Job(conf, "Cell Oriented Approach MR1");
        job.setJarByClass(FirstMapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(ID.class);
        job.setMapOutputValueClass(Tuple.class);
        job.setOutputKeyClass(ID.class);
        job.setOutputValueClass(Tuple.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        DBInputFormat.setInput(
                job,
                DBInputTableRecord.class,
                "input",   //input table name
                null,
                null,
                new String[]{"objId", "trajId", "t", "lon", "lat", "x", "y"}  // table columns
        );


        SequenceFileOutputFormat.setOutputPath(job, new Path(System.getProperty("user.dir") + "/output/celloriented/fmr")); //set via args

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static double FirstMRStartTime;
    static double FirstMRElapsedTimeInSec;

}