package org.apache.pinot.core.query.aggregation.utils.hashset;

import java.nio.ByteBuffer;
import java.util.AbstractSet;
import java.util.Iterator;


/**
 * OffHeapHashSet for Integer that uses OffHeapHashSetManager
 */
public class OffHeapIntHashSet extends AbstractSet<Integer> {

  OffHeapHashSetManager _hsm;

  OffHeapIntHashSet() {
    _hsm = new OffHeapHashSetManager(new FixedLengthOffHeapPageManager(1000000, 4), 10000, 100);
  }

  byte[] toBytes(Integer data) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(data.intValue());
    return buffer.array();
  }

  @Override
  public boolean add(Integer data) {
    _hsm.insertData(toBytes(data), data.hashCode()); //insertData requires data to be in bytes
    return true;
  }

  @Override
  public boolean contains(Object value) {
    Integer data = (Integer) value;
    return _hsm.contains(toBytes(data), data.hashCode()); //contains requires data to be in bytes
  }

  // use page manager iterator but convert returned serialized bytes[] to int
  @Override
  public Iterator<Integer> iterator() {
    Iterator<Integer> it = new Iterator<Integer>() {
      Iterator<byte[]> pageIterator = _hsm._pageManager.iterator();

      @Override
      public boolean hasNext() {
        return pageIterator.hasNext();
      }

      @Override
      public Integer next() {
        byte[] value = pageIterator.next(); //returns serialized data as byte[]
        ByteBuffer buffer = ByteBuffer.wrap(value);
        return buffer.getInt();
      }
    };
    return it;
  }

  @Override
  public int size() {
    return _hsm._pageManager._numKeys;
  }
}
