import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ParallelSScan extends RecursiveAction {
    private double[] data;
    private int segmentSize;

    private double[] r;
    private double[] scans;

    private final int THRESHOLD = 1000;
    private int start;
    private int end;

    private int numSegments;

    private class Reduce extends RecursiveAction {

        @Override
        protected void compute() {
            if(end - start < THRESHOLD) {
                reduce(start, numSegments);
                return;
            } else {
                int mid = start + (end - start) / 2;
                var left = new ParallelSScan(data, start, mid);
                left.fork();
                var right = new ParallelSScan(data, mid, end);
                right.compute();
                left.join();
            }
        } 
    }

    private class Scan extends RecursiveAction {
        @Override
        protected void compute() {
            if(end - start < THRESHOLD) {
                scan(start, numSegments, 0);
                return;
            } else {
                int mid = start + (end - start) / 2;
                var left = new ParallelSScan(data, start, mid);
                left.fork();
                var right = new ParallelSScan(data, mid, end);
                right.compute();
                left.join();
            }
        }
    }

    public ParallelSScan(double[] data, int start, int end) {
        this.data = data;
        this.start = start;
        this.end = end;
        this.segmentSize = end - start;
        numSegments = 1 + (data.length - 1)/segmentSize;
        r = new double[numSegments];
        scans = Arrays.copyOf(data, data.length);
    }

    public double[] getScans() {
        return scans;
    }

    public double[] plainScan() {
        double[] ss = Arrays.copyOf(data, data.length);
        ss[0] = slow(ss[0]);
        for (int i=1; i<data.length; ++i)
            ss[i] = slow(ss[i]) + ss[i - 1];
        return ss;
    }

    public void recursiveReduce() {
        // this is the number of segments. The last segment might be short
        numSegments = 1 + (data.length - 1)/segmentSize;

        r = new double[numSegments];
        scans = Arrays.copyOf(data, data.length); // we could overwrite the original, or make a copy.

        reduce(0, numSegments);
    }

    // This will be doing all of the reduces across the entire array
    private void reduce(int start, int numSegments) {
        if (numSegments == 1) {
            // base case. Compute a reduction over the ith segment.
            r[start] = segmentReduce(start*segmentSize);
        }
        else {
            int k = split(numSegments);
            reduce(start, k);
            reduce(start+k, numSegments-k);

            if (numSegments == 2*k) {
                r[start+numSegments-1] = r[start+k-1] + r[start+numSegments-1];
            }
        }
    }

    // This will be doing all of the scans across the entire array
    private void scan(int start, int numSegments, double initial) {
        if (numSegments == 1) {
            // base case; compute the scan over the ith segment
            segmentScan(start*segmentSize, initial);
        }
        else {
            int k = split(numSegments);
            scan(start, k, initial);
            initial = initial + r[start+k-1];
            scan(start+k, numSegments-k, initial);
        }
    }

    private int split(int m) {
        return Integer.highestOneBit(m-1);
    }

    private double segmentReduce(int start) {
        int end = start + segmentSize <= data.length ? start+segmentSize : data.length;
        double r = 0.0;
        for (int i=start; i<end; ++i)
            r += slow(data[i]);
        return r;
    }

    private void segmentScan(int start, double initial) {
        int end = start + segmentSize <= data.length ? start+segmentSize : data.length;

        scans[start] = slow(scans[start]) + initial;
        for (int i=start+1; i<end;++i)
            scans[i] = slow(scans[i]) + scans[i-1];
    }

    // this is here to artifically increase the computational intensity
    private double slow(double d) {
        double d2 = d;
        while (d2 > 0) {
            d2 -= 3000000;
        }
        return d;
    }

    public static void main(String[] args) {
        double[] data = new double[1_000_000];
        for(int i = 0; i < data.length; i++) {
            data[i] = i;
        }
        long start = System.currentTimeMillis();
        ParallelSScan parallelSScan = new ParallelSScan(data, 0, data.length);
        Reduce reduce = parallelSScan.new Reduce();
        ForkJoinPool.commonPool().invoke(reduce);
        Scan scan = parallelSScan.new Scan();
        ForkJoinPool.commonPool().invoke(scan);
        long end = System.currentTimeMillis() - start;
        System.out.println("The total time for the parallel recursive version is: " + end + " ms");
        double[] parallelData = parallelSScan.getScans();
    }

    @Override
    protected void compute() {
    }
}
