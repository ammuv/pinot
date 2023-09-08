package org.apache.pinot.core.query.aggregation.utils.hashset;

import java.util.AbstractSet;
import java.util.Iterator;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * OffHeapHashSet for byte[] that uses OffHeapHashSetManager
 */
public class OffHeapByteHashSet extends AbstractSet<byte[]> {

  OffHeapHashSetManager _hsm;

  OffHeapByteHashSet() {
    _hsm = new OffHeapHashSetManager(new VariableLengthOffHeapPageManager(1000000), 10000, 100);
  }

  @Override
  public boolean add(byte[] data) {
    _hsm.insertData(data, new ByteArray(data).hashCode());
    return true;
  }

  @Override
  public boolean contains(Object value) {
    byte[] data = (byte[]) value;
    return _hsm.contains(data, new ByteArray(data).hashCode());
  }

  @Override
  public Iterator<byte[]> iterator() {
    return _hsm._pageManager.iterator();
  }

  @Override
  public int size() {
    return _hsm._pageManager._numKeys;
  }
}
