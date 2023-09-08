package org.apache.pinot.core.query.aggregation.utils.hashset;

import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Off-heap Hash Array Implementation Each Bucket of the Hash Array will be of size 12 bytes and contain
 * (hashcode,PageAddress) in serialized form
 */
public class OffHeapHashBuckets {
  PinotDataBuffer _buckets;

  OffHeapHashBuckets(int size) {
    _buckets = PinotDataBuffer.allocateDirect(size * 4 * 3, PinotDataBuffer.NATIVE_ORDER, null);
  }

  boolean isEmptyBucket(int bucketIdx) {
    return _buckets.getInt(bucketIdx * 12) == 0 && _buckets.getInt(bucketIdx * 12 + 4) == 0
        && _buckets.getInt(bucketIdx * 12 + 8) == 0;
  }

  PageAddress getPageAddress(int bucketIdx) {
    int bufNum = _buckets.getInt(bucketIdx * 12 + 4);
    int bufOffset = _buckets.getInt(bucketIdx * 12 + 8);
    return new PageAddress(bufNum, bufOffset);
  }

  int getHashCode(int bucketIdx) {
    return _buckets.getInt(bucketIdx * 12);
  }

  // returns true if hash codes are the same
  boolean compare(int hashcode, int bucketIdx) {
    return (hashcode == getHashCode(bucketIdx));
  }

  void insert(int hashcode, PageAddress addr, int bucketIdx) {
    _buckets.putInt(bucketIdx * 12, hashcode);
    _buckets.putInt(bucketIdx * 12 + 4, addr.bufNum);
    _buckets.putInt(bucketIdx * 12 + 8, addr.bufOffset);
  }
}
