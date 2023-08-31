package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.kudu.Type;
import org.apache.seatunnel.connectors.seatunnel.kudu.utils.KuduColumn;

import java.util.ArrayList;

/*
Slice each kuduScanner according to the bucketCapacity: Perform row-based splitting on each scanner(keeping track
of the maximum and minimum values for each scanner, and split them into equal rows based on the bucket capacity).
 */
public class KuduScannerRowsSplit {
    private ArrayList<Object> scannerRowsSplitBucket;
    private final int maxBucketNum;
    private int bucketCapacity;
    private boolean firstBucketSplit = false;
    private boolean lastBucketSplit = false;
    private final Type keyType;

    public KuduScannerRowsSplit(int maxBucketNum, int bucketCapacity, Type keyType) {
        this.maxBucketNum = maxBucketNum;
        this.bucketCapacity = bucketCapacity;
        this.keyType = keyType;
        scannerRowsSplitBucket = new ArrayList<>((maxBucketNum >> 2 > 0 ? maxBucketNum >> 2 : 1) + 1);
    }

    public int getBucketNum() {
        if (getSize() == 0) {
            return 0;
        } else {
            return getSize() - 1;
        }
    }

    public int addBucketCapacity(int bucketCapacity) {
        this.bucketCapacity += bucketCapacity;
        return this.bucketCapacity;
    }

    public boolean isFirstBucketSplit() {
        return firstBucketSplit;
    }

    public boolean isLastBucketSplit() {
        return lastBucketSplit;
    }

    public int getSize() {
        return scannerRowsSplitBucket.size();
    }

    public int getBucketCapacity() {
        return bucketCapacity;
    }

    public Object getFirst() {
        return scannerRowsSplitBucket.get(0);
    }

    public Object getLast() {
        return scannerRowsSplitBucket.get(getSize() - 1);
    }

    public Object get(int index) {
        if (index < 0 || index >= getSize()) {
            return null;
        }
        return scannerRowsSplitBucket.get(index);
    }

    public void add(Object intervalValue) {
        if (getSize() == 0) {
            /*Set the lower bound of the bucket*/
            scannerRowsSplitBucket.add(intervalValue);
        }
        int compare = -1;
        int i = 0;
        for ( ; i < scannerRowsSplitBucket.size(); i++) {
            compare = KuduColumn.KeyColCompare(intervalValue, scannerRowsSplitBucket.get(i), keyType);
            if (compare <= 0) {
                break;
            }
        }
        if(compare != 0) {
            scannerRowsSplitBucket.add(i, intervalValue);
        }
        if (isBucketNumFull()) {
            extendBucketCapacity();
        }
    }

    public void clear() {
        scannerRowsSplitBucket.clear();
    }

    /* Remove values less than 'end' from 'scannerRowsSplitBucket' */
    public void removeLesser(Object start) {
        int index = 0;
        for (; index < getSize(); index++) {
            if (KuduColumn.KeyColCompare(start, scannerRowsSplitBucket.get(index), keyType) < 0) {
                break;
            }
        }
        if (index >= getSize()) {
            scannerRowsSplitBucket.clear();
        } else {
            ArrayList<Object> tmpScannerRowsSplitBucket = new ArrayList<>();
            if (index != 0 && KuduColumn.KeyColCompare(start, scannerRowsSplitBucket.get(index), keyType) != 0) {
                /* Set lower bound */
                tmpScannerRowsSplitBucket.add(start);
                /* bucket has split */
                firstBucketSplit = true;
            }
            tmpScannerRowsSplitBucket.addAll(scannerRowsSplitBucket.subList(index, getSize()));
            scannerRowsSplitBucket.clear();
            /* Two values are needed to form a bucket */
            if(tmpScannerRowsSplitBucket.size() > 1) {
                scannerRowsSplitBucket = tmpScannerRowsSplitBucket;
                if (getBucketNum() == 1) {
                    lastBucketSplit = firstBucketSplit;
                }
            }
        }
    }

    /* Split 'KuduScannerRowsSplit' by values less than or equal to 'end' */
    public KuduScannerRowsSplit getLesser(Object end) {
        KuduScannerRowsSplit newScannerRowsSplit = new KuduScannerRowsSplit(maxBucketNum, bucketCapacity, keyType);
        /*The firstBucketSplit as before*/
        newScannerRowsSplit.firstBucketSplit = firstBucketSplit;
        for (Object bucket : scannerRowsSplitBucket) {
            if (KuduColumn.KeyColCompare(bucket, end, keyType) <= 0) {
                newScannerRowsSplit.add(bucket);
            }
        }
        if (newScannerRowsSplit.getSize() > 0
                && newScannerRowsSplit.getSize() < getSize()
                && KuduColumn.KeyColCompare(newScannerRowsSplit.getLast(), end, keyType) != 0) {
            /* Set upper bound, have split */
            newScannerRowsSplit.add(end);
            newScannerRowsSplit.lastBucketSplit = true;
            if (newScannerRowsSplit.getBucketNum() == 1) {
                newScannerRowsSplit.firstBucketSplit =true;
            }
        } else if (newScannerRowsSplit.getSize() == getSize()) {
            /* No split performed, preserving the original values */
            newScannerRowsSplit.lastBucketSplit = lastBucketSplit;
        }
        /* Two values are needed to form a bucket */
        if (newScannerRowsSplit.getBucketNum() == 0) {
            newScannerRowsSplit.clear();
        }
        return newScannerRowsSplit;
    }

    /*Check if the bucket count is full*/
    public boolean isBucketNumFull() {
        return getBucketNum() >= maxBucketNum;
    }

    /*
    The scannerRowsSplitBucket is used up and
    the bucketCapacity needs to be increased to reduce the size of the scannerRowsSplitBucket
     */
    public boolean extendBucketCapacity() {
        int tmpBucketCapacity = bucketCapacity;
        if (this.getBucketNum() >= 2) {
            ArrayList<Object> tmpScannerRowsSplitBucket = new ArrayList<>(this.getSize());
            /*Keep the lower bound*/
            tmpScannerRowsSplitBucket.add(scannerRowsSplitBucket.get(0));
            int index = 2;
            for (; index < this.getBucketNum(); index += 2) {
                Object tmpIntervalValue = scannerRowsSplitBucket.get(index);
                tmpScannerRowsSplitBucket.add(tmpIntervalValue);
            }
            /* The last bucket is not expanded */
            if (index != this.getBucketNum()) {
                lastBucketSplit = true;
            }
            /*Keep the upper bound.*/
            tmpScannerRowsSplitBucket.add(scannerRowsSplitBucket.get(this.getBucketNum()));
            scannerRowsSplitBucket.clear();
            scannerRowsSplitBucket = tmpScannerRowsSplitBucket;
            if (getBucketNum() == 1) {
                firstBucketSplit = lastBucketSplit = false;
            }
            bucketCapacity *= 2;
        }
        return tmpBucketCapacity < bucketCapacity;
    }

}
