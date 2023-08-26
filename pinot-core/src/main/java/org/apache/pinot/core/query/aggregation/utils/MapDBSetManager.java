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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.data.FieldSpec;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;


/**
 * An ID generator to produce a global unique identifier for each query, used in v1/v2 engine for tracking and
 * inter-stage communication(v2 only). It's guaranteed by:
 * <ol>
 *   <li>
 *     Using a mask computed using the hash-code of the broker-id to ensure two brokers don't arrive at the same
 *     requestId. This mask becomes the most significant 9 digits (in base-10).
 *   </li>
 *   <li>
 *     Using a auto-incrementing counter for the least significant 9 digits (in base-10).
 *   </li>
 * </ol>
 */
class MapDBIdGenerator {
  private static final long OFFSET = 1_000_000_000L;
  private final long _mask;
  private final AtomicLong _incrementingId = new AtomicLong(0);

  public MapDBIdGenerator() {
    _mask = ((long) (Integer.MAX_VALUE)) * OFFSET;
  }

  public String get() {
    long normalized = (_incrementingId.getAndIncrement() & Long.MAX_VALUE) % OFFSET;
    return String.valueOf(_mask + normalized);
  }
}

public class MapDBSetManager implements HashSetManager {

  private DB _db;
  // private MapDBIdGenerator _idGenerator;
  long _id;

  public MapDBSetManager() {
    _db = DBMaker.memoryDirectDB().make();
    //_idGenerator = new MapDBIdGenerator();
    _id = 0;
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    return getHashSet(valueType);
  }

  private String getId() {
    ++_id;
    return String.valueOf(_id);
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType) {
    switch (valueType) {
      case INT:
        return _db.hashSet(getId()).serializer(Serializer.INTEGER).createOrOpen();
      case LONG:
        return _db.hashSet(getId()).serializer(Serializer.LONG).createOrOpen();
      case FLOAT:
        return _db.hashSet(getId()).serializer(Serializer.FLOAT).createOrOpen();
      case DOUBLE:
        return _db.hashSet(getId()).serializer(Serializer.DOUBLE).createOrOpen();
      case STRING:
        return _db.hashSet(getId()).serializer(Serializer.STRING).createOrOpen();
      case BYTES:
        return _db.hashSet(getId()).serializer(Serializer.BYTE_ARRAY).createOrOpen();
      default:
        throw new IllegalStateException("Illegal data type for MapDB Hash Set" + valueType);
    }
  }

  @Override
  public Set<Record> getRecordHashSet(int size) {
    return (Set<Record>) (_db.hashSet(getId()).serializer(Serializer.JAVA).createOrOpen());
  }

  @Override
  /**
   * Serializes a HashSet into a byte array.
   */ public byte[] toBytes(Set set) {
    int size = set.size();

    //Directly return the size (0) for empty set
    if (size == 0) {
      return new byte[Integer.BYTES];
    }

    // Besides the value bytes, we store: size, key type and length for each key
    Object key = set.iterator().next();
    if (key instanceof Integer) {
      return toBytes(set, HashSetManager.SerDataType.Integer);
    } else if (key instanceof Long) {
      return toBytes(set, HashSetManager.SerDataType.Long);
    } else if (key instanceof Float) {
      return toBytes(set, HashSetManager.SerDataType.Float);
    } else if (key instanceof Double) {
      return toBytes(set, HashSetManager.SerDataType.Double);
    } else if (key instanceof String) {
      return toBytes(set, HashSetManager.SerDataType.String);
    } else if (key instanceof byte[]) {
      return toBytes(set, HashSetManager.SerDataType.Bytes);
    } else {
      throw new IllegalStateException("Key Type of Hash Set not supported for serialization" + key.getClass());
    }
  }
}
