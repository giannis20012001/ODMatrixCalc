package org.lumi.odmatrixcalc.celloriented;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.lumi.odmatrixcalc.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public class SecondMapReduce extends Configured implements Tool {
    public static class MyMapper extends Mapper<ID, Tuple, ID, Tuple> {
        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            SecondMRStartTime = System.currentTimeMillis();

        }

        @Override
        public void map(final ID cellId, final Tuple trajIdAndPoints, final Mapper.Context context) throws IOException, InterruptedException {
            ID trajId = ((SerializableWrapper<ID>) trajIdAndPoints.get(0)).getObject();
            Tuple<SerializableWrapper> cellIdAndPoints = new Tuple<>(2);
            cellIdAndPoints.add(new SerializableWrapper<ID>(cellId));
            cellIdAndPoints.add((SerializableWrapper<SpatialTemporalPoint>) trajIdAndPoints.get(1));
            context.write(trajId, cellIdAndPoints);

        }

        @Override
        public void cleanup(Mapper.Context context) throws IOException, InterruptedException{
            SecondMRElapsedTimeInSec = (System.currentTimeMillis() - SecondMRStartTime);
            System.out.println("Map() took " + SecondMRElapsedTimeInSec + " milliseconds.");

        }

    }

    public static class MyReducer extends Reducer<ID, Tuple, Tuple, ID> {
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            SecondMRStartTime=System.currentTimeMillis();

        }

        @Override
        public void reduce(ID trajId, Iterable<Tuple> tuples, Context context) throws IOException, InterruptedException {
            ArrayList<Tuple> list = new ArrayList<>();

            for (Tuple<SerializableWrapper> tuple : tuples) {
                ID cellId = ((SerializableWrapper<ID>) tuple.get(0)).getObject();
                Tuple<SpatialTemporalPoint> points = ((SerializableWrapper<Tuple>) tuple.get(1)).getObject();

                for (SpatialTemporalPoint point : points) {
                    Tuple<Wrapper> tAndCellId = new Tuple<>(2);
                    tAndCellId.add(new Wrapper<Long>(point.getT()));
                    tAndCellId.add(new Wrapper<ID>(cellId));
                    list.add(tAndCellId);
                }
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
                    Tuple<SerializableComparableWrapper> cellOCellDAndJC = new Tuple<>(3);
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
                Tuple<SerializableComparableWrapper> cellOCellDAndJC = new Tuple<>(3);
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
            SecondMRElapsedTimeInSec = (System.currentTimeMillis() - SecondMRStartTime);
            System.out.println("Reduce() took " + SecondMRElapsedTimeInSec + " milliseconds.");

        }

    }

    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "Cell Oriented Approach MR2");

        job.setJarByClass(SecondMapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(ID.class);
        job.setMapOutputValueClass(Tuple.class);
        job.setOutputKeyClass(Tuple.class);
        job.setOutputValueClass(ID.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(System.getProperty("user.dir") + "/output/celloriented/fmr"));
        SequenceFileOutputFormat.setOutputPath(job, new Path(System.getProperty("user.dir") + "/output/celloriented/smr/"));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static long SecondMRStartTime;
    static long SecondMRElapsedTimeInSec;

}
