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

import java.util.HashMap;
import java.util.Set;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * There can be multiple possible (offheap and onheap) HashSetTypes such as OpenHashSet, ChronicleSet, ...
 * HashSetFactory has: - enum of possible HashSet types - HashSetManager for each possible type - Mapping for which
 * HashSet type will be used for each data type (this will be based on Config) HashSetFactory is used for: - obtaining
 * new HashSets given a datatype - returning HashSetManagers
 */
public class HashSetFactory {

  /**
   * Enum for possible HashSet types
   */
  enum HashSetType {
    OpenHashSet, ChronicleSet, MapDBSet, DictSet
  }

  /**
   * Map specifying which HashSet type to use for each data type
   */
  public HashMap<FieldSpec.DataType, HashSetType> _typeToSetMap;

  /**
   * Map storing the HashSetManagers for all the HashSetTypes
   */
  public HashMap<HashSetType, HashSetManager> _hashSetManagerMap;

  public HashSetType _hashSetTypeForRecord;

  // todo: use PinotConfig to configure DataType to HashSetTypes map
  public HashSetFactory() {
    _typeToSetMap = new HashMap<>(6);
    HashSetType c = HashSetType.ChronicleSet;
    HashSetType o = HashSetType.OpenHashSet;
    HashSetType m = HashSetType.MapDBSet;
    HashSetType d = HashSetType.DictSet;

    _typeToSetMap.put(FieldSpec.DataType.INT, d);
    _typeToSetMap.put(FieldSpec.DataType.LONG, d);
    _typeToSetMap.put(FieldSpec.DataType.FLOAT, d);
    _typeToSetMap.put(FieldSpec.DataType.DOUBLE, d);
    _typeToSetMap.put(FieldSpec.DataType.STRING, d);
    _typeToSetMap.put(FieldSpec.DataType.BYTES, d);

    _hashSetManagerMap = new HashMap<>(4);
    _hashSetManagerMap.put(HashSetType.ChronicleSet, new ChronicleSetManager());
    _hashSetManagerMap.put(HashSetType.OpenHashSet, new OpenHashSetManager());
    _hashSetManagerMap.put(HashSetType.MapDBSet, new MapDBSetManager());
    _hashSetManagerMap.put(HashSetType.DictSet, new SetFromDictionaryManager());
    //_hashSetTypeForRecord = HashSetType.ChronicleSet;
    //_hashSetTypeForRecord = HashSetType.OpenHashSet;
    _hashSetTypeForRecord = HashSetType.DictSet;
  }

  /**
   * Returns a new hashSet for a given DataType
   */
  public Set getHashSet(FieldSpec.DataType valueType) {

    if (_typeToSetMap.containsKey(valueType)) {
      HashSetManager hm = _hashSetManagerMap.get(_typeToSetMap.get(valueType));
      return hm.getHashSet(valueType);
    } else {
      throw new IllegalStateException("Illegal data type for Hash Set " + valueType);
    }
  }

  /**
   * Returns a new hashSet of a specified size for a given DataType
   */
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    if (_typeToSetMap.containsKey(valueType)) {
      HashSetManager hm = _hashSetManagerMap.get(_typeToSetMap.get(valueType));
      return hm.getHashSet(valueType, size);
    } else {
      throw new IllegalStateException("Illegal data type for Hash Set " + valueType);
    }
  }

  /**
   * Returns the stored HashSetManager for the HashSet type corresponding to a given data type
   */
  public HashSetManager getHasHSetManager(FieldSpec.DataType valueType) {
    if (_typeToSetMap.containsKey(valueType)) {
      return _hashSetManagerMap.get(_typeToSetMap.get(valueType));
    } else {
      throw new IllegalStateException("Illegal data type for Hash Set" + valueType);
    }
  }

  /**
   * Returns a new hashSet with keys as Records of a specified size If the parameter isOpenHashSet is true, it returns
   * OpenHashSet Otherwise it returns the HashSet according to the configured _hashSetTypeForRecord
   */
  public Set<Record> getRecordHashSet(boolean isOpenHashSet, int size) {
    if (isOpenHashSet) {
      return _hashSetManagerMap.get(HashSetType.OpenHashSet).getRecordHashSet(size);
    } else {
      return _hashSetManagerMap.get(_hashSetTypeForRecord).getRecordHashSet(size);
    }
  }
}
