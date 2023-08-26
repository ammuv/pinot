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
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.utils.ByteArray;


public class BytesOffHeapSetFromDictionary extends AbstractSet<byte[]> {
  private final BytesOffHeapMutableDictionary _dict;

  public BytesOffHeapSetFromDictionary(int estimatedCardinality, int maxOverflowSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, int avgLen) {
    _dict = new BytesOffHeapMutableDictionary(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext,
        avgLen);
  }

  @Override
  public boolean add(byte[] value) {
    _dict.index(value);
    return true;
  }

  @Override
  public boolean contains(Object value) {
    if (_dict.indexOf(new ByteArray((byte[]) value)) == Dictionary.NULL_VALUE_INDEX) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public Iterator<byte[]> iterator() {
    Iterator<byte[]> it = new Iterator<byte[]>() {
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < _dict.length();
      }

      @Override
      public byte[] next() {
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