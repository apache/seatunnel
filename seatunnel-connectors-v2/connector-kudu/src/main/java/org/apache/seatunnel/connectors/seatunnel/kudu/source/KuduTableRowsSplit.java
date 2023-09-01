/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.kudu.Type;

import org.apache.seatunnel.connectors.seatunnel.kudu.utils.KuduColumn;

import java.util.ArrayList;
import java.util.LinkedList;


/*All shards of kudu table*/
public class KuduTableRowsSplit {
    private LinkedList<KuduScannerRowsSplit> kuduTableRowsSplit = new LinkedList<>();
    private int maxBucketCapacity = 0;
    private final int maxBucketNum;
    private final Type keyType;

    public KuduTableRowsSplit(int maxBucketNum, Type keyType) {
        this.keyType = keyType;
        this.maxBucketNum = maxBucketNum << 1;
    }

    public int getBucketNum() {
        int bucketNum = 0;
        for (KuduScannerRowsSplit kuduScannerRowsSplit : kuduTableRowsSplit) {
            bucketNum += kuduScannerRowsSplit.getBucketNum();
        }
        return bucketNum;
    }

    /* Add split buckets of KuduScanner */
    public void add(KuduScannerRowsSplit rowsSplit) {
        if (rowsSplit.getBucketNum() == 0) {
            return;
        }
        int index = 0;
        boolean isEnd = false;
        LinkedList<KuduScannerRowsSplit> tmpKuduScannerRowsSplit = new LinkedList<>(kuduTableRowsSplit);
        for (KuduScannerRowsSplit scannerRowsSplit : tmpKuduScannerRowsSplit) {
            Object first = scannerRowsSplit.getFirst();
            Object last = scannerRowsSplit.getLast();
            if (KuduColumn.KeyColCompare(rowsSplit.getLast(), first, keyType) <= 0) {
                kuduTableRowsSplit.add(index, rowsSplit);
                isEnd = true;
                break;
            } else if (KuduColumn.KeyColCompare(rowsSplit.getFirst(), first, keyType) < 0) {
                addScannerBucketCapacity(index, rowsSplit.getBucketCapacity() >> 1);
                KuduScannerRowsSplit lesserRowsSplit = rowsSplit.getLesser(first);
                kuduTableRowsSplit.add(index++, lesserRowsSplit);
                rowsSplit.removeLesser(last);
            } else if (KuduColumn.KeyColCompare(rowsSplit.getFirst(), last, keyType) < 0) {
                addScannerBucketCapacity(index, rowsSplit.getBucketCapacity() >> 1);
                rowsSplit.removeLesser(last);
            }
            if (rowsSplit.getBucketNum() == 0) {
                isEnd = true;
                break;
            }
            index++;
        }
        tmpKuduScannerRowsSplit.clear();
        if (!isEnd) {
            kuduTableRowsSplit.addLast(rowsSplit);
        }
        int bucketNum = getBucketNum();
        /* if bucketNum is full or bucketCapacity is not equal, need extend  */
        if(bucketNum >= maxBucketNum || maxBucketCapacity != rowsSplit.getBucketCapacity()) {
            int tmpMaxBucketCapacity = maxBucketCapacity;
            if(bucketNum >= maxBucketNum) {
                tmpMaxBucketCapacity *= 2;
            }
            if (tmpMaxBucketCapacity <= rowsSplit.getBucketCapacity()) {
                maxBucketCapacity = rowsSplit.getBucketCapacity();
                for (KuduScannerRowsSplit scannerRowsSplit : kuduTableRowsSplit) {
                    extendScannerBucketCapacity(scannerRowsSplit, maxBucketCapacity);
                }
            } else {
                for (KuduScannerRowsSplit kuduScannerRowsSplit : kuduTableRowsSplit) {
                    boolean isSuccess = extendScannerBucketCapacity(kuduScannerRowsSplit, tmpMaxBucketCapacity);
                    if (isSuccess) {
                        maxBucketCapacity = tmpMaxBucketCapacity;
                    }
                }
            }
        }
    }

    private void addScannerBucketCapacity(int index, int bucketCapacity) {
        KuduScannerRowsSplit newScannerRowsSplit = kuduTableRowsSplit.get(index);
        int addBucketCapacity = newScannerRowsSplit.addBucketCapacity(bucketCapacity);
        if((maxBucketCapacity << 1) < addBucketCapacity) {
            maxBucketCapacity <<= 1;
        }
    }

    private boolean extendScannerBucketCapacity(KuduScannerRowsSplit kuduScannerRowsSplit, int extendBucketCapacity) {
        int bucketCapacity = kuduScannerRowsSplit.getBucketCapacity();
        while (bucketCapacity < extendBucketCapacity) {
            boolean isExtend = kuduScannerRowsSplit.extendBucketCapacity();
            if(!isExtend) {
                break;
            }
            bucketCapacity = kuduScannerRowsSplit.getBucketCapacity();
        }
        return bucketCapacity >= extendBucketCapacity;
    }


    /* Merge the buckets for each KuduScanner */
    public ArrayList<Object> getRangeValue() {
        ArrayList<Object> rangeValue = new ArrayList<>();
        int preBucketCapacity = 0;
        Object lastValue = null;
        for (KuduScannerRowsSplit kuduScannerRowsSplit : kuduTableRowsSplit) {
            int bucketCapacity = kuduScannerRowsSplit.getBucketCapacity();
            if (rangeValue.size() == 0) {
                rangeValue.add(kuduScannerRowsSplit.get(0));
            }
            for(int i = 1; i < kuduScannerRowsSplit.getSize(); i++) {
                if((i == 1 && kuduScannerRowsSplit.isFirstBucketSplit())
                        || (i == kuduScannerRowsSplit.getBucketNum() && kuduScannerRowsSplit.isLastBucketSplit())) {
                    /* The capacity of sliced buckets is halved */
                    preBucketCapacity += bucketCapacity >> 1;
                } else {
                    preBucketCapacity += bucketCapacity;
                }
                /* rangeValue add:
                The accumulated capacity of the buckets is equal to or greater than the maxBucketCapacity */
                if (preBucketCapacity >= maxBucketCapacity) {
                    Object value = kuduScannerRowsSplit.get(i);
                    rangeValue.add(value);
                    preBucketCapacity = 0;
                }
            }
            lastValue = kuduScannerRowsSplit.getLast();
        }
        if(rangeValue.size() > 0) {
            lastValue = KuduColumn.addValue(keyType, lastValue);
            if (preBucketCapacity < (maxBucketCapacity >> 1)) {
                rangeValue.remove(rangeValue.size() - 1);
            }
            rangeValue.add(lastValue);
        }
        return rangeValue;
    }
}
