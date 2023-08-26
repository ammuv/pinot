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
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * HashSetManager for ChronicleSet
 */
public class ChronicleSetManager implements HashSetManager {

  // Various ChronicleSet specific parameters
  // todo: figure out a better way to set them using PinotConfig
  static int _defaultSizeChronicleSet = 50000;
  static Float _defaultAvgKeyFloat = (float) 10000.9999;
  static String _defaultAvgKeyString = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

  static int _defaultAvgKeySize = 5000;
  static byte[] _defaultAvgKeyByte = new byte[30];

  @Override
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    size = _defaultSizeChronicleSet;
    switch (valueType) {
      case INT:
        return ChronicleSetBuilder.of(Integer.class).entries(size).create();
      case LONG:
        return ChronicleSetBuilder.of(Long.class).entries(size).create();
      case FLOAT:
        return ChronicleSetBuilder.of(Float.class).averageKey(_defaultAvgKeyFloat).entries(size).create();
      case DOUBLE:
        return ChronicleSetBuilder.of(Double.class).entries(size).create();
      case STRING:
        return ChronicleSetBuilder.of(String.class).averageKey(_defaultAvgKeyString).entries(size).create();
      case BYTES:
        return ChronicleSetBuilder.of(byte[].class).averageKey(_defaultAvgKeyByte).entries(size).create();
      default:
        throw new IllegalStateException("Illegal data type for Chronicle Hash Set");
    }
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType) {
    return getHashSet(valueType, _defaultSizeChronicleSet);
  }

  @Override
  public Set<Record> getRecordHashSet(int size) {
    size = _defaultSizeChronicleSet; //we want to ensure that the set can grow
    return ChronicleSetBuilder.of(Record.class).averageKeySize(_defaultAvgKeySize).entries(size).create();
  }

  @Override
  public byte[] toBytes(Set set) {

    ChronicleSet cset = (ChronicleSet) set;

    // Besides the value bytes, we store: size, key type and length for each key
    if (cset.keyType().equals(Integer.class)) {
      return toBytes(set, HashSetManager.SerDataType.Integer);
    } else if (cset.keyType().equals(Long.class)) {
      return toBytes(set, HashSetManager.SerDataType.Long);
    } else if (cset.keyType().equals(Float.class)) {
      return toBytes(set, HashSetManager.SerDataType.Float);
    } else if (cset.keyType().equals(Double.class)) {
      return toBytes(set, HashSetManager.SerDataType.Double);
    } else if (cset.keyType().equals(String.class)) {
      return toBytes(set, HashSetManager.SerDataType.String);
    } else if (cset.keyType().equals(byte[].class)) {
      return toBytes(set, SerDataType.Bytes);
    } else {
      throw new IllegalStateException("Key Type of Hash Set not supported for serialization" + cset.keyClass());
    }
  }
}
