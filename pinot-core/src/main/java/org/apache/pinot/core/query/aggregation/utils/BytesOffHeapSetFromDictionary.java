package org.apache.pinot.core.query.aggregation.utils;

import java.util.AbstractSet;
import java.util.Iterator;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.utils.ByteArray;


public class BytesOffHeapSetFromDictionary extends AbstractSet<byte[]>{
    private final BytesOffHeapMutableDictionary _dict;

    public BytesOffHeapSetFromDictionary(int estimatedCardinality, int maxOverflowSize,
        PinotDataBufferMemoryManager memoryManager, String allocationContext,int avgLen){
      _dict = new BytesOffHeapMutableDictionary(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext, avgLen);
    }

    @Override
    public boolean add(byte[] value) {
      _dict.index(value);
      return true;
    }

    @Override
    public boolean contains(Object value){
      if(_dict.indexOf(new ByteArray((byte[])value)) == Dictionary.NULL_VALUE_INDEX)
        return false;
      else
        return true;
    }

    @Override
    public Iterator<byte[]> iterator() {
      Iterator<byte[]> it = new Iterator<byte[]>(){
        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
          return currentIndex < _dict.length();
        }

        @Override
        public byte[] next(){
          byte[] value = _dict.getBytesValue(currentIndex);
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

