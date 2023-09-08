package org.apache.pinot.core.query.aggregation.utils.hashset;

import java.io.IOException;


/**
 * HashSetManager class manages contains a PageManager and also manages the hashing aspects of an offHeapHashSet
 */
public class OffHeapHashSetManager {
  BaseOffHeapPageManager _pageManager;
  int _numBuckets;
  int _linearProbeLimit;
  OffHeapHashBuckets _buckets;

  OffHeapHashSetManager(BaseOffHeapPageManager pageManager, int numBuckets, int linearProbeLimit) {
    _pageManager = pageManager;
    _numBuckets = numBuckets;
    _linearProbeLimit = linearProbeLimit;
  }

  int hash(int hashcode) {
    return hashcode % _numBuckets;
  }

  //helper for expand function
  boolean insertWithoutContainsCheck(int hashcode, PageAddress addr, OffHeapHashBuckets buckets, int numBuckets) {
    int startIdx = hash(hashcode);
    int bucketIdx = startIdx;
    if (bucketIdx < numBuckets && bucketIdx < (startIdx + _linearProbeLimit) && !_buckets.isEmptyBucket(bucketIdx)) {
      bucketIdx++;
    }
    if (bucketIdx == (startIdx + _linearProbeLimit) || bucketIdx == numBuckets) {
      return false;
    }
    buckets.insert(hashcode, addr, bucketIdx);
    return true;
  }

  /**
   * Expand function to grow off-heap hash array.
   * The function is called when data added exceeds probelimit or when the hash array is full
   */
  void expand(byte[] data, int hashcode) {
    int newNumBuckets = _numBuckets;
    PageAddress addr = _pageManager.addData(data);
    OffHeapHashBuckets newBuckets = null;

    boolean isProbeLimitReached = true;
    while (true && isProbeLimitReached) {
      newNumBuckets *= 2;

      if (newBuckets != null) {
        try {
          newBuckets._buckets.release();
        } catch (IOException e) {
          throw new RuntimeException("problem while releasing direct memory in offHeapHashSet");
        }
      }

      newBuckets = new OffHeapHashBuckets(newNumBuckets);

      insertWithoutContainsCheck(hashcode, addr, newBuckets, newNumBuckets);

      int bucketIdx = 0;
      int hc;
      PageAddress pageAddr;
      isProbeLimitReached = false;

      while (bucketIdx < _numBuckets && !isProbeLimitReached) {
        if (!_buckets.isEmptyBucket(bucketIdx)) {
          hc = _buckets.getHashCode(bucketIdx);
          pageAddr = _buckets.getPageAddress(bucketIdx);
          if (!insertWithoutContainsCheck(hc, pageAddr, newBuckets, newNumBuckets)) {
            isProbeLimitReached = true;
            break;
          }
          bucketIdx++;
        }
      }
    }
    _numBuckets = newNumBuckets;
    _buckets = newBuckets;
  }

  boolean contains(byte[] data, int hashcode) {
    int startIdx = hash(hashcode);
    int bucketIdx = getNextIdx(data, hashcode, startIdx);
    if (bucketIdx < (startIdx + _linearProbeLimit) && !_buckets.isEmptyBucket(bucketIdx)) {
      return true;
    }
    return false;
  }

  // compare called only if data present at bucketIdx
  boolean compare(byte[] data, int hashcode, int bucketIdx) {
    if (_buckets.compare(hashcode, bucketIdx) == false) {
      return false;
    } else {
      PageAddress addr = _buckets.getPageAddress(bucketIdx);
      return _pageManager.compareData(addr, data);
    }
  }


  // make it boolean and handle memory overflow
  void insertData(byte[] data, int hashcode) {
    int startIdx = hash(hashcode);
    int bucketIdx = getNextIdx(data, hashcode, startIdx);
    if (bucketIdx < _numBuckets && bucketIdx < (startIdx + _linearProbeLimit) && !_buckets.isEmptyBucket(
        bucketIdx)) { //data already exists
      return;
    } else if (bucketIdx >= startIdx + _linearProbeLimit || bucketIdx >= _numBuckets) //need to expand to get bucketIdx
    {
      expand(data, hashcode);
      return;
    }
    // can be inserted at bucketIdx
    PageAddress addr = _pageManager.addData(data);
    _buckets.insert(hashcode, addr, bucketIdx);
  }

  // Helper function for contains and insert.
  int getNextIdx(byte[] data, int hashcode, int bucketIdx) {
    int probeCount = 0;
    while (probeCount < _linearProbeLimit && bucketIdx < _numBuckets) {
      if (_buckets.isEmptyBucket(bucketIdx) || compare(data, hashcode, bucketIdx) == true) {
        return bucketIdx;
      }
      bucketIdx++;
      probeCount++;
    }
    return bucketIdx;
  }
}
