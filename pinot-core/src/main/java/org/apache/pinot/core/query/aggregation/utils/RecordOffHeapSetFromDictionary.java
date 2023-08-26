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

import java.util.AbstractSet;
import java.util.Iterator;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


public class RecordOffHeapSetFromDictionary extends AbstractSet<Record> {
  private final RecordOffHeapMutableDictionary _dict;

  public RecordOffHeapSetFromDictionary(int estimatedCardinality, int maxOverflowSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, int avgLen) {
    _dict = new RecordOffHeapMutableDictionary(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext,
        avgLen);
  }

  @Override
  public boolean add(Record value) {
    _dict.index(value);
    return true;
  }

  @Override
  public boolean contains(Object value) {
    if (_dict.indexOf((Record) value) == Dictionary.NULL_VALUE_INDEX) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public Iterator<Record> iterator() {
    Iterator<Record> it = new Iterator<Record>() {
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < _dict.length();
      }

      @Override
      public Record next() {
        Record value = _dict.getRecordValue(currentIndex);
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
