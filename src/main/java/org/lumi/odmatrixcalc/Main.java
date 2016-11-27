package org.lumi.odmatrixcalc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.lumi.odmatrixcalc.initialization.DBInputCreator;
import org.lumi.odmatrixcalc.initialization.MinMax;
import org.lumi.odmatrixcalc.initialization.MinMaxFinder;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 10/1/2016.
 */

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int returnCode = 0;
        int numberOfCells;
        double start;
        double elapsedTimeInSec;
        Scanner scanner = new Scanner(System.in);

        int cores = Runtime.getRuntime().availableProcessors();
        System.out.printf("Available cores: %d\n\n", cores);
        System.out.print("Enter choice (1-CellOriented, 2-TrajectoryOriented): ");
        int choice = Integer.parseInt(scanner.nextLine());
        switch (choice) {
            case 1:
                System.out.println("Executting CellOriented solution");
                System.out.printf("Enter number of cells to set precision (default 8): ");
                numberOfCells = Integer.parseInt(scanner.nextLine()); //TODO: Enter evaluation for blank entry.
                start = System.currentTimeMillis();
                returnCode = celloriented(args, conf, returnCode, numberOfCells);
                // Segment to monitor
                elapsedTimeInSec = (System.currentTimeMillis() - start);
                System.out.printf("Totatl execution time: %d sec\n", TimeUnit.MILLISECONDS.toSeconds((long) elapsedTimeInSec));
                break;

            case 2:
                System.out.println("Executting TrajectoryOriented solution");
                System.out.printf("Enter number of cells to set precision (default 8): ");
                numberOfCells = Integer.parseInt(scanner.nextLine()); //TODO: Enter evaluation for blank entry.
                start = System.currentTimeMillis();
                returnCode = trajectoryoriented(args, conf, returnCode, numberOfCells);
                // Segment to monitor
                elapsedTimeInSec = (System.currentTimeMillis() - start);
                System.out.printf("Totatl execution time: %d sec\n", TimeUnit.MILLISECONDS.toSeconds((long) elapsedTimeInSec));
                break;

            default:
                System.out.println("You did not enter correct choice value!!!");
                break;

        }

        System.exit(returnCode);

    }

    private static int celloriented(String[] args, Configuration conf, int returnCode, int numberOfCells) throws Exception {
        //First phase
        System.out.println("Initializing first phase");
        //returnCode = ToolRunner.run(conf, new DBInputCreator(), args);
        if (returnCode == 0) {
            //Second phase
            System.out.println("The Database Input Creator phase is done.");
            System.out.println("Initializing second phase");
            MinMax minMax = (new MinMaxFinder()).execute(conf, args);

            //Third phase
            System.out.println("The Min Max Finder phase is done.");
            System.out.println("Initializing third phase.");
            String myArgs[] = new String[7];
            myArgs[0] = String.valueOf(minMax.getMinX());
            myArgs[1] = String.valueOf(minMax.getMaxX());
            myArgs[2] = String.valueOf(minMax.getMinY());
            myArgs[3] = String.valueOf(minMax.getMaxY());
            myArgs[4] = String.valueOf(minMax.getMinT());
            myArgs[5] = String.valueOf(minMax.getMaxT());
            myArgs[6] = String.valueOf(numberOfCells);
            returnCode = ToolRunner.run(conf, new org.lumi.odmatrixcalc.celloriented.FirstMapReduce(), myArgs);

            if (returnCode == 0) {
                //Fourth phase
                System.out.println("The FisrtMapReduce phase is done.");
                System.out.println("Initializing fourth phase.");
                returnCode = ToolRunner.run(conf, new org.lumi.odmatrixcalc.celloriented.SecondMapReduce(), args); //args or myArgs ?
                if (returnCode == 0) {
                    //Fifth phase
                    System.out.println("The SecondMapReduce phase is done.");
                    System.out.println("Initializing fifth phase.");
                    returnCode = ToolRunner.run(conf, new org.lumi.odmatrixcalc.celloriented.ThirdMapReduce(), args);
                    System.out.println("The ThirdMapReduce phase is done.");

                }

            }

        }

        return returnCode;

    }

    private static int trajectoryoriented(String[] args, Configuration conf, int returnCode, int numberOfCells) throws Exception {
        //First phase
        System.out.println("Initializing first phase");
        //returnCode = ToolRunner.run(conf, new DBInputCreator(), args);
        if (returnCode == 0) {
            //Second phase
            System.out.println("The Database Input Creator phase is done.");
            System.out.println("Initializing second phase");
            MinMax minMax = (new MinMaxFinder()).execute(conf, args);

            //Third phase
            System.out.println("The Min Max Finder phase is done.");
            System.out.println("Initializing third phase.");
            String myArgs[] = new String[7];
            myArgs[0] = String.valueOf(minMax.getMinX());
            myArgs[1] = String.valueOf(minMax.getMaxX());
            myArgs[2] = String.valueOf(minMax.getMinY());
            myArgs[3] = String.valueOf(minMax.getMaxY());
            myArgs[4] = String.valueOf(minMax.getMinT());
            myArgs[5] = String.valueOf(minMax.getMaxT());
            myArgs[6] = String.valueOf(numberOfCells);
            returnCode = ToolRunner.run(conf, new org.lumi.odmatrixcalc.trajectoryoriented.FirstMapReduce(), myArgs);

            if (returnCode == 0) {
                //Fourth phase
                System.out.println("The FisrtMapReduce phase is done.");
                System.out.println("Initializing fourth phase.");
                returnCode = ToolRunner.run(conf, new org.lumi.odmatrixcalc.trajectoryoriented.SecondMapReduce(), args); //args or myArgs ?
                System.out.println("The SecondMapReduce phase is done.");

            }

        }

        return returnCode;

    }

}
