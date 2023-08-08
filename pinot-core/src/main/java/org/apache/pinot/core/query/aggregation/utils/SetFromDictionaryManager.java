package org.apache.pinot.core.query.aggregation.utils;

import java.util.Set;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;


public class SetFromDictionaryManager implements HashSetManager{

  private static PinotDataBufferMemoryManager _memoryManager;
  private static int _defaultSizeSet = 2000;

  public SetFromDictionaryManager(){
     _memoryManager = new DirectMemoryManager(HashSetManager.class.getName());
  }
  @Override
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    switch (valueType) {
      case INT:
        return new IntOffHeapSetFromDictionary(size, 5000, _memoryManager, "intColumn");
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return new StringOffHeapSetFromDictionary(size, 5000, _memoryManager, "stringColumn",100);
      case BYTES:
        return new BytesOffHeapSetFromDictionary(size, 5000, _memoryManager, "stringColumn",100);
      default:
        throw new IllegalStateException("Illegal data type for Chronicle Hash Set");
    }
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType) {
    return getHashSet(valueType, _defaultSizeSet);
  }

  @Override
  public Set<Record> getRecordHashSet(int size) {
    throw new IllegalStateException("Illegal data type for Chronicle Hash Set");
  }
}
