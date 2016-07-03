package org.lumi.odmatrixcalc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by John Tsantilis (A.K.A lumi) on 2/7/2016.
 */

public class SequenceFileOperator {
    public void readSequenceFile(String sequenceFilePath) throws IOException {
        // TODO Auto-generated method stub

        /*
         * SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(fs,
         * new Path(sequenceFilePath), conf);
         */
        SequenceFile.Reader.Option filePath = SequenceFile.Reader.file(new Path(sequenceFilePath));
        SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath);
        Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getValueClass(), conf);

        try {
            while (sequenceFileReader.next(key, value)) {
                System.out.printf("[%s] %s %s \n", sequenceFileReader.getPosition(), key, value.getClass());

            }

        }
        finally {
            IOUtils.closeStream(sequenceFileReader);

        }

    }

    public void loadDocumentsToSequenceFile(String docDirectoryPath, String sequenceFilePath) throws IOException {
        // TODO Auto-generated method stub
        File docDirectory = new File(docDirectoryPath);
        if (!docDirectory.isDirectory()) {
            System.out.println("Please provide an absolute path of a directory that contains the documents to be added to the sequence file");

            return;

        }

        /*
         * SequenceFile.Writer sequenceFileWriter =
         * SequenceFile.createWriter(fs, conf, new Path(sequenceFilePath),
         * Text.class, BytesWritable.class);
         */
        org.apache.hadoop.io.SequenceFile.Writer.Option filePath = SequenceFile.Writer.file(new Path(sequenceFilePath));
        org.apache.hadoop.io.SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);
        org.apache.hadoop.io.SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(BytesWritable.class);

        SequenceFile.Writer sequenceFileWriter = SequenceFile.createWriter(conf, filePath, keyClass, valueClass);
        File[] documents = docDirectory.listFiles();

        try {
            for (File document : documents) {
                RandomAccessFile raf = new RandomAccessFile(document, "r");
                byte[] content = new byte[(int) raf.length()];
                raf.readFully(content);
                sequenceFileWriter.append(new Text(document.getName()), new BytesWritable(content));
                raf.close();

            }

        }
        finally {
            IOUtils.closeStream(sequenceFileWriter);

        }

    }

    /**
     * Constructor
     */
    public SequenceFileOperator(Configuration conf, String[] args, int operation) throws IOException {
        setConf(conf);
        setArgs(args);
        if (this.args == null || this.args.length < 2) {
            System.out.println("Following are the possible invocations <operation id> <arg0> <arg0> ...");
            System.out.println("1 <absolute path of directory containing documents> <HDFS path of the sequence file");
            System.out.println("2 <HDFS path of the sequence file>");

            return;

        }

        //SequenceFileOperator docToSeqFileWriter = new SequenceFileOperator();

        switch (operation) {
            case 1: {
                String docDirectoryPath = this.args[0];
                String sequenceFilePath = this.args[1];
                System.out.println("Writing files present at " + docDirectoryPath + " to the sequence file " + sequenceFilePath);
                loadDocumentsToSequenceFile(docDirectoryPath, sequenceFilePath);
                break;

            }

            case 2: {
                String sequenceFilePath = this.args[0];
                System.out.println("Reading the sequence file " + sequenceFilePath);
                readSequenceFile(sequenceFilePath);
                break;

            }

        }

    }

    /**
     * Getters & Setters
     */
    public Configuration getConf() {
        return conf;

    }

    public void setConf(Configuration conf) {
        this.conf = conf;

    }

    public String[] getArgs() {
        return args;

    }

    public void setArgs(String[] args) {
        this.args = args;

    }

    private Configuration conf = new Configuration();
    private String[] args;

    /*private FileSystem fs;
    {
        try {
            fs = FileSystem.get(URI.create("hdfs://cldx-1336-1202:9000"), conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }*/

}
