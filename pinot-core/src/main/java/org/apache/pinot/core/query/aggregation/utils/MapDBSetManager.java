package org.apache.pinot.core.query.aggregation.utils;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.utils.HashSetManager;
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

public class MapDBSetManager implements HashSetManager{

  private DB _db;
 // private MapDBIdGenerator _idGenerator;
  long _id;

  public MapDBSetManager(){
    _db = DBMaker.memoryDirectDB().make();
    //_idGenerator = new MapDBIdGenerator();
    _id = 0;
  }
  @Override
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    return getHashSet(valueType);
  }

  private String getId(){
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
    return (Set<Record>)(_db.hashSet(getId()).serializer(Serializer.JAVA).createOrOpen());
  }
}
