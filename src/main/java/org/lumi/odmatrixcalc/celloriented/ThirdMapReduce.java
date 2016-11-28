package org.lumi.odmatrixcalc.celloriented;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.lumi.odmatrixcalc.util.ID;
import org.lumi.odmatrixcalc.util.Result;
import org.lumi.odmatrixcalc.util.SerializableComparableWrapper;
import org.lumi.odmatrixcalc.util.Tuple;

import java.io.IOException;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public class ThirdMapReduce extends Configured implements Tool {
    public static class MyMapper extends Mapper<Tuple, ID, Tuple, ID> {
        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            ThirdMRStartTime = System.currentTimeMillis();

        }

        @Override
        public void map(final Tuple cellOCellDAndJC, final ID trajId, final Mapper.Context context) throws IOException, InterruptedException {
            context.write(cellOCellDAndJC, trajId);

        }

        @Override
        public void cleanup(Mapper.Context context) throws IOException, InterruptedException{
            ThirdMRElapsedTimeInSec = (System.currentTimeMillis() - ThirdMRStartTime);
            System.out.println("Map() took " + ThirdMRElapsedTimeInSec + " milliseconds.");

        }

    }

    @SuppressWarnings("Duplicates")
    public static class MyReducer extends Reducer<Tuple, ID, Result, NullWritable> {
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            ThirdMRStartTime=System.currentTimeMillis();

        }

        @Override
        public void reduce(Tuple cellOCellDAndJC, Iterable<ID> trajIds, Context context) throws IOException, InterruptedException {
            Tuple<ID> trajIdList = new Tuple<>();

            Result result = new Result();
            result.setCellIdO(((SerializableComparableWrapper<ID>) cellOCellDAndJC.get(0)).getObject());
            result.setCellIdD(((SerializableComparableWrapper<ID>) cellOCellDAndJC.get(1)).getObject());

            for (ID<Tuple> trajId : trajIds) {
                trajIdList.add(new ID<>(trajId.getId()));
            }

            result.setTrajIds(trajIdList);
            result.setJobCode(Result.JobCodes.valueOf(((SerializableComparableWrapper<String>) cellOCellDAndJC.get(2)).getObject()));
            context.write(result, NullWritable.get());

        }

        @Override
        public void cleanup(Reducer.Context context) throws IOException, InterruptedException{
            ThirdMRElapsedTimeInSec = (System.currentTimeMillis() - ThirdMRStartTime);
            System.out.println("Reduce() took " + ThirdMRElapsedTimeInSec + " milliseconds.");

        }

    }

    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "Cell Oriented Approach MR3");

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",   // driver class
                "jdbc:mysql://192.168.100.100:3306/testDb?autoReconnect=true&useSSL=false", // db url
                "mlk",    // user name
                "!1q2w3e!"); //password

        job.setJarByClass(ThirdMapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(ID.class);
        job.setOutputKeyClass(Result.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(System.getProperty("user.dir") + "/output/celloriented/smr"));
        DBOutputFormat.setOutput(
                job,
                "results",    // output table name
                new String[]{"jobCode", "cellIdO", "cellIdD", "trajIds", "count"}   //table columns
        );

        return job.waitForCompletion(true) ? 0 : 1;

    }

    static long ThirdMRStartTime;
    static long ThirdMRElapsedTimeInSec;

}
