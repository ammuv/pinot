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

import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


public class RecordOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {
  private final MutableOffHeapByteArrayStore _byteStore;

  public RecordOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, int avgLen) {
    super(estimatedCardinality, maxOverflowHashSize, memoryManager, allocationContext);
    _byteStore = new MutableOffHeapByteArrayStore(memoryManager, allocationContext, estimatedCardinality, avgLen);
  }

  @Override
  public int index(Object value) {
    Record recordValue = (Record) value;
    return indexValue(recordValue, recordValue.toBytes());
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      Record recordValue = (Record) values[i];
      dictIds[i] = indexValue(recordValue, recordValue.toBytes());
    }
    return dictIds;
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return ByteArray.compare(getBytesValue(dictId1), getBytesValue(dictId2));
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    throw new UnsupportedOperationException(" getDictIdsInRange not supported for Record Dict");
  }

  @Override
  public ByteArray getMinVal() {
    throw new UnsupportedOperationException("Min not supported for Record Dict");
  }

  @Override
  public ByteArray getMaxVal() {
    throw new UnsupportedOperationException("Min not supported for Record Dict");
  }

  public ByteArray[] getSortedValues() {
    throw new UnsupportedOperationException("getSortedValues() not supported for Record Dict");
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return FieldSpec.DataType.BYTES; // Internally record stored as bytes - RECORD is not a DataType in Pinot
  }

  @Override
  public int indexOf(String stringValue) {
    throw new UnsupportedOperationException("indexOf(String) not supported for Record Dict");
  }

  public int indexOf(Record recordValue) {
    return getDictId(recordValue, recordValue.toBytes());
  }

  @Override
  public Record get(int dictId) {
    return getRecordValue(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _byteStore.get(dictId);
  }

  public Record getRecordValue(int dictId) {
    return Record.fromBytes(_byteStore.get(dictId));
  }

  @Override
  protected void setValue(int dictId, Object value, byte[] serializedValue) {
    _byteStore.add(serializedValue);
  }

  @Override
  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return _byteStore.equalsValueAt(serializedValue, dictId);
  }

  @Override
  public int getAvgValueSize() {
    return (int) _byteStore.getAvgValueSize();
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    return getOffHeapMemUsed() + _byteStore.getTotalOffHeapMemUsed();
  }

  @Override
  public void doClose()
      throws IOException {
    _byteStore.close();
  }
}
