package org.lumi.odmatrixcalc.trajectoryoriented;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.lumi.odmatrixcalc.initialization.DBInputTableRecord;
import org.lumi.odmatrixcalc.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 2/7/2016.
 */

public class FirstMapReduce extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, DBInputTableRecord, ID, Tuple> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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

            time=System.currentTimeMillis();

        }

        @Override
        public void map(final LongWritable key, final DBInputTableRecord record, final Context context) throws IOException, InterruptedException {
            SpatialTemporalPoint point = new SpatialTemporalPoint(record.getX(), record.getY(), record.getT());
            Tuple<Long> trajIdAsTuple = new Tuple(2);
            trajIdAsTuple.add(record.getObjId());
            trajIdAsTuple.add(record.getTrajId());

            Tuple<SerializableWrapper> cellIdIdAndPoint = new Tuple<>(2);
            cellIdIdAndPoint.add(new SerializableWrapper<>(grid.getCellId(point)));
            cellIdIdAndPoint.add(new SerializableWrapper<>(point));

            context.write(new ID<>(trajIdAsTuple), cellIdIdAndPoint);

        }

        @Override
        public void cleanup(Mapper.Context context) throws IOException, InterruptedException{
            end=System.currentTimeMillis();
            time = end - time;
            System.out.println("Map() took " + TimeUnit.MILLISECONDS.toSeconds(time) + " sec.");

        }

        private SpatialTemporalGrid grid;

    }

    public static class MyReducer extends Reducer<ID, Tuple, Tuple, ID> {
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            time=System.currentTimeMillis();

        }

        @Override
        public void reduce(ID trajId, Iterable<Tuple> tuples, Context context) throws IOException, InterruptedException {
            ArrayList<Tuple> list = new ArrayList<>();

            for (Tuple<SerializableWrapper> tuple : tuples) {
                ID cellId = ((SerializableWrapper<ID>) tuple.get(0)).getObject();
                SpatialTemporalPoint point = ((SerializableWrapper<SpatialTemporalPoint>) tuple.get(1)).getObject();

                Tuple<Wrapper> tAndCellId = new Tuple<>(2);
                tAndCellId.add(new Wrapper<Long>(point.getT()));
                tAndCellId.add(new Wrapper<ID>(cellId));
                list.add(tAndCellId);

            }

            Collections.sort(list, new Comparator<Object>() {
                @Override
                public int compare(Object o1, Object o2) {
                    Long t1 = ((Wrapper<Long>) ((Tuple<Wrapper>) o1).get(0)).getObject();
                    Long t2 = ((Wrapper<Long>) ((Tuple<Wrapper>) o2).get(0)).getObject();
                    return (int) (t1 - t2);

                }

            });

            //What to do if we have only one cell ?
            for (int i = 0; i < list.size() - 1; i++) {
                ID<Tuple> cellOId = ((Wrapper<ID>) list.get(i).get(1)).getObject();
                ID<Tuple> cellDId = ((Wrapper<ID>) list.get(i + 1).get(1)).getObject();
                if (!cellOId.equals(cellDId)) {
                    Tuple<SerializableWrapper> cellOCellDAndJC = new Tuple<>(3);
                    cellOCellDAndJC.add(new SerializableComparableWrapper<>(cellOId));
                    cellOCellDAndJC.add(new SerializableComparableWrapper<>(cellDId));
                    cellOCellDAndJC.add(new SerializableComparableWrapper<>(Result.JobCodes.PREVIOUS_NEXT_JOB.name()));

                    /************Debugging Code****************/
                    /*System.err.println("<-------------START------------->");
                    System.err.println("JobCode: START_END_JOB");
                    System.err.println("CellIdO -> CellIdD, trajId: " + cellOCellDAndJC.get(0).getObject() + "-> " + cellOCellDAndJC.get(1).getObject() + "," + trajId);
                    System.err.println("<--------------END-------------->");
                    System.err.println();*/
                    /*****************************************/


                    context.write(cellOCellDAndJC, trajId);

                }

            }

            if (list.size() > 1) {
                ID<Tuple> cellOId = ((Wrapper<ID>) list.get(0).get(1)).getObject();
                ID<Tuple> cellDId = ((Wrapper<ID>) list.get(list.size() - 1).get(1)).getObject();
                Tuple<SerializableWrapper> cellOCellDAndJC = new Tuple<>(3);
                cellOCellDAndJC.add(new SerializableComparableWrapper<>(cellOId));
                cellOCellDAndJC.add(new SerializableComparableWrapper<>(cellDId));
                cellOCellDAndJC.add(new SerializableComparableWrapper<>(Result.JobCodes.START_END_JOB.name()));

                /************Debugging Code****************/
                /*System.err.println("<-------------START------------->");
                System.err.println("JobCode: PREVIOUS_NEXT_JOB");
                System.err.println("CellIdO -> CellIdD, trajId: " + cellOCellDAndJC.get(0).getObject() + "-> " + cellOCellDAndJC.get(1).getObject() + "," + trajId);
                System.err.println("<--------------END-------------->");
                System.err.println();*/
                /*****************************************/

                context.write(cellOCellDAndJC, trajId);

            }

        }

        @Override
        public void cleanup(Reducer.Context context) throws IOException, InterruptedException{
            end=System.currentTimeMillis();
            time = end - time;
            System.out.println("Reduce() took " + TimeUnit.MILLISECONDS.toSeconds(time) + " sec.");

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
        /*conf.set("4","mapreduce.tasktracker.map.tasks.maximum");
        conf.set("4", "mapreduce.tasktracker.reduce.tasks.maximum");*/
        //conf.set("8","yarn.nodemanager.resource.cpu-vcores");

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
        job.setOutputKeyClass(Tuple.class);
        job.setOutputValueClass(ID.class);
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

        //Here we can put HDFS notation in order to use the distributed FS provided by Hadoop
        SequenceFileOutputFormat.setOutputPath(job, new Path(System.getProperty("user.dir") + "/output/trajectoryoriented/fmr")); //set via args

        int completion = job.waitForCompletion(true) ? 0 : 1;
        //Find time completion for map() & reduce() part
        TaskReport[] mapReports = job.getTaskReports(TaskType.MAP);
        for(TaskReport report : mapReports) {
            long time = report.getFinishTime() - report.getStartTime();
            System.out.println(report.getTaskId() + " map() took " + TimeUnit.MILLISECONDS.toSeconds(time) + " sec!");

        }

        TaskReport[] reduceReports = job.getTaskReports(TaskType.REDUCE);
        for(TaskReport report : reduceReports) {
            long time = report.getFinishTime() - report.getStartTime();
            System.out.println(report.getTaskId() + " reduce() took " + TimeUnit.MILLISECONDS.toSeconds(time) + " sec");

        }

        return completion;

    }

    static long time;
    static long end;

}