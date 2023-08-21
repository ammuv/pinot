package org.apache.pinot.core.query.aggregation.utils;

import java.util.AbstractSet;
import java.util.Iterator;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


public class IntOffHeapSetFromDictionary extends AbstractSet<Integer> {
  public final IntOffHeapMutableDictionary _dict;

  public IntOffHeapSetFromDictionary(int estimatedCardinality, int maxOverflowSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext){
    _dict = new IntOffHeapMutableDictionary(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext);
  }
  @Override
  public boolean add(Integer value) {
    _dict.index(value);
    return true;
  }

  @Override
  public boolean contains(Object value){
    if(_dict.indexOf((int)value)==Dictionary.NULL_VALUE_INDEX)
      return false;
    else
      return true;
  }

  @Override
  public Iterator<Integer> iterator() {
    Iterator<Integer> it = new Iterator<Integer>(){
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < _dict.length();
      }

      @Override
      public Integer next(){
        int value = _dict.getIntValue(currentIndex);
        currentIndex++;
        return value;
      }
    };
    return it;
  }

  @Override
  public int size() {
    return _dict.length();
  }
}
