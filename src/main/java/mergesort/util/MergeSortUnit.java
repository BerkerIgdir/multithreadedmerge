package mergesort.util;

public class MergeSortUnit {
    public final int rangeEnd;
    public final int mergePoint;
    public final int rangeStart;


    public MergeSortUnit(int rangeStart, int rangeEnd, int mergePoint) {
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.mergePoint = mergePoint;
    }
}
