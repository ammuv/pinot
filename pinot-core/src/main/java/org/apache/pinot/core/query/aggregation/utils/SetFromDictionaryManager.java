/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.utils;

import java.util.Set;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;


public class SetFromDictionaryManager implements HashSetManager {

  private static PinotDataBufferMemoryManager _memoryManager;
  private static int _defaultSizeSet = 10000; //1mb worth page

  public SetFromDictionaryManager() {
    _memoryManager = new DirectMemoryManager(HashSetManager.class.getName());
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    size = _defaultSizeSet;
    switch (valueType) {
      case INT:
        return new IntOffHeapSetFromDictionary(size, 0, _memoryManager, null);
      case LONG:
        return new LongOffHeapSetFromDictionary(size / 2, 0, _memoryManager, null);
      case FLOAT:
        return new FloatOffHeapSetFromDictionary(size, 0, _memoryManager, null);
      case DOUBLE:
        return new DoubleOffHeapSetFromDictionary(size / 2, 0, _memoryManager, null);
      case STRING:
        return new StringOffHeapSetFromDictionary(size, 0, _memoryManager, null, 1);
      case BYTES:
        return new BytesOffHeapSetFromDictionary(size, 0, _memoryManager, null, 1);
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
    return new RecordOffHeapSetFromDictionary(_defaultSizeSet, 0, _memoryManager, null, 1);
  }
}
