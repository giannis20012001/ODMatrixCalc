package org.lumi.odmatrixcalc.util;

/**
 * Created by lumi (A.K.A. John Tsantilis) on 4/6/2016.
 */

public class SpatialTemporalGrid {

    public SpatialTemporalGrid(int numCells, double minX, double maxX, double minY, double maxY, long minT, long maxT) {

        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.minT = minT;
        this.maxT = maxT;

        lengthPerDimension = (Math.cbrt(numCells) == (int) Math.cbrt(numCells) ? (int) Math.cbrt(numCells) : -1);

        if (lengthPerDimension == -1)
            throw new NotValidNumberOfCellsExceptions(lengthPerDimension);

        this.xCellRange = (maxX - minX) / numCells;
        this.yCellRange = (maxY - minY) / numCells;
        this.tCellRange = (long) ((maxT - minT) / (double) numCells);
    }


    public ID<Tuple<Integer>> getCellId(SpatialTemporalPoint point) {

        if (point.getX() < minX || point.getX() > maxX
                || point.getY() < minY || point.getY() > maxY
                || point.getT() < minT || point.getT() > maxT)
            throw new PointOutOfGridException(point);

        Tuple<Integer> tuple = new Tuple<>(3);
        int indexX = (int) Math.ceil((point.getX() - minX) / xCellRange);
        int indexY = (int) Math.ceil((point.getY() - minY) / yCellRange);
        int indexT = (int) Math.ceil((point.getT() - minT) / (double) tCellRange);

        if (indexX == 0) indexX = 1;
        else if (indexX > lengthPerDimension) indexX = lengthPerDimension; /* The last cell may have larger x, y or t range */

        if (indexY == 0) indexY = 1;
        else if (indexY > lengthPerDimension) indexY = lengthPerDimension;

        if (indexT == 0) indexT = 1;
        else if (indexT > lengthPerDimension) indexT = lengthPerDimension;

        tuple.add(indexX);
        tuple.add(indexY);
        tuple.add(indexT);

        return new ID<Tuple<Integer>>(tuple);

    }

    private double xCellRange;
    private double yCellRange;
    private long tCellRange;
    private double minX;
    private double maxX;
    private double minY;
    private double maxY;
    private long minT;
    private long maxT;
    private int lengthPerDimension;


    public class PointOutOfGridException extends RuntimeException {

        public PointOutOfGridException(SpatialTemporalPoint notValidPoint) {
            super("Point " + notValidPoint.toString() + "is out of grid");
        }
    }

    public class NotValidNumberOfCellsExceptions extends RuntimeException {

        public NotValidNumberOfCellsExceptions(int notValidNumCells) {
            super(notValidNumCells + " has not a perfect cubic root");
        }
    }

}
