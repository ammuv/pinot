package org.apache.pinot.core.query.aggregation.utils;

import java.util.AbstractSet;
import java.util.Iterator;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


public class StringOffHeapSetFromDictionary extends AbstractSet<String> {
  private final StringOffHeapMutableDictionary _dict;

  public StringOffHeapSetFromDictionary(int estimatedCardinality, int maxOverflowSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext,int avgStringLen){
    _dict = new StringOffHeapMutableDictionary(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext, avgStringLen);
  }

  @Override
  public boolean add(String value) {
    _dict.index(value);
    return true;
  }

  @Override
  public boolean contains(Object value){
    if(_dict.indexOf((String)value)== Dictionary.NULL_VALUE_INDEX)
      return false;
    else
      return true;
  }

  @Override
  public Iterator<String> iterator() {
    Iterator<String> it = new Iterator<String>(){
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < _dict.length();
      }

      @Override
      public String next(){
        String value = _dict.getStringValue(currentIndex);
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
