import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ParallelScan {
    private double[] data;
    private int segmentSize;

    private double[] r;
    private double[] scans;

    private class Reduce extends RecursiveAction {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private int numSegments;
        private int start;

        public Reduce(int start, int numSegments) {
            this.start = start;
            this.numSegments = numSegments;
        }

        @Override
        protected void compute() {
            if (numSegments == 1) {
                // base case. Compute a reduction over the ith segment.
                r[start] = segmentReduce(start*segmentSize);
            }
            else {
                int k = split(numSegments);
                var left = new Reduce(start, k);
                left.fork();
                var right = new Reduce(start+k, numSegments-k);
                right.compute();
                left.join();
    
                if (numSegments == 2*k) {
                    r[start+numSegments-1] = r[start+k-1] + r[start+numSegments-1];
                }
            }
        } 
    }

    private class Scan extends RecursiveAction {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private int numSegments;
        private int start;
        private double initial;

        public Scan(int start, int numSegments, double intiial) {
            this.start = start;
            this.numSegments = numSegments;
            this.initial = initial;
        }

        @Override
        protected void compute() {
            if (numSegments == 1) {
                // base case; compute the scan over the ith segment
                segmentScan(start*segmentSize, initial);
            }
            else {
                int k = split(numSegments);
                var left = new Scan(start, k, initial);
                left.fork();
                initial = initial + r[start+k-1];
                var right = new Scan(start+k, numSegments-k, initial);
                right.compute();
                left.join();
            }
        }
    }

    public ParallelScan(double[] data, int segmentSize) {
        this.data = data;
        this.segmentSize = segmentSize;
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

    public void recursiveScan() {
        ForkJoinPool pool = new ForkJoinPool();
        // this is the number of segments. The last segment might be short
        int numSegments = 1 + (data.length - 1)/segmentSize;

        r = new double[numSegments];
        scans = Arrays.copyOf(data, data.length); // we could overwrite the original, or make a copy.
        pool.invoke(new Reduce(0, numSegments));
        pool.invoke(new Scan(0, numSegments, 0));
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
        double[] data = new double[100];
        for(int i = 0; i < data.length; i++) {
            data[i] = i;
        }
        ParallelScan scan = new ParallelScan(data, 10);
        long start = System.currentTimeMillis();
        scan.recursiveScan();
        long end = System.currentTimeMillis() - start;
        System.out.println("The total time to execute the scan patter in parallel is: " + end + " ms");
    }
}
