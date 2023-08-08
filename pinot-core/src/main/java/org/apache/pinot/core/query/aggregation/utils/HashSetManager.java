package org.apache.pinot.core.query.aggregation.utils;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.data.FieldSpec;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Interface for HashSetManagers of various HashSet Types such as OpenHashSet, ChronicleMap, ... It provides methods for
 * adding to HashSets according to data types, serializing, deserializing and obtaining new Hash Sets
 * <p>
 * Notes: We have functions to add values to a given HashSet for various data types. The implementation will ensure that
 * boxing unboxing is not done unnecessarily where possible. See for example OpenHashSetManager.
 */
public interface HashSetManager {

  /**
   * Returns a new hashSet of a for a given data type
   */
  public Set getHashSet(FieldSpec.DataType valueType);

  /**
   * Returns a new hashSet of a specified size for a given data type
   */
  public Set getHashSet(FieldSpec.DataType valueType, int size);

  /**
   * Returns a new hashSet with keys as Records of a specified size
   */
  public Set<Record> getRecordHashSet(int size);

  default public void addInteger(Set set, int value) {
    set.add(value);
  }

  default public void addFloat(Set set, float value) {
    set.add(value);
  }

  default public void addDouble(Set set, double value) {
    set.add(value);
  }

  default public void addLong(Set set, long value) {
    set.add(value);
  }

  default public void addString(Set set, String value) {
    set.add(value);
  }

  default public void addObject(Set set, Object value) {
    set.add(value);
  }

  default public void addInteger(Set set, int[] values) {
    for (int value : values) {
      set.add(value);
    }
  }

  default public void addFloat(Set set, float[] values) {
    for (float value : values) {
      set.add(value);
    }
  }

  default public void addDouble(Set set, double[] values) {
    for (double value : values) {
      set.add(value);
    }
  }

  default public void addLong(Set set, long[] values) {
    for (long value : values) {
      set.add(value);
    }
  }

  default public void addString(Set set, String[] values) {
    for (String value : values) {
      set.add(value);
    }
  }

  default public void addObject(Set set, Object[] values) {
    for (Object value : values) {
      set.add(value);
    }
  }

  default public void addBytes(Set set, byte[] values) {
    set.add(values);
  }

  /**
   * Enum for the various possible data types supported by the HashSets for serialization and deserialization purposes
   */
  public enum SerDataType {
    Integer(0), Long(1), Float(2), Double(3), String(4), Bytes(5);
    private final int _value;

    SerDataType(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }
  }


  /**
   * Serializes a HashSet into a byte array.
   */
  public default byte[] toBytes(Set set) {
    int size = set.size();

    //Directly return the size (0) for empty set
    if (size == 0) {
      return new byte[Integer.BYTES];
    }

    // Besides the value bytes, we store: size, key type and length for each key
    Object key = set.iterator().next();
    if (key instanceof Integer){
      return toBytesFixedLengthType(set,HashSetManager.SerDataType.Integer);
    } else if(key instanceof Long){
      return toBytesFixedLengthType(set,HashSetManager.SerDataType.Long);
    } else if(key instanceof Float){
      return toBytesFixedLengthType(set,HashSetManager.SerDataType.Float);
    } else if(key instanceof Double){
      return toBytesFixedLengthType(set,HashSetManager.SerDataType.Double);
    } else if (key instanceof String) {
      return toBytesString((Set<String>) set);
    } else if (key instanceof byte[]) {
      return toBytesByteArray((Set<byte[]>) set);
    } else {
      throw new IllegalStateException("Key Type of Chronicle Hash Set not supported for serialization" + key.getClass());
    }
  }

  /**
   * Deserializes a HashSet from a {@link ByteBuffer}.
   */
  public default Set fromByteBuffer(ByteBuffer byteBuffer) {

    // For fixed value types we store: keyType, size, valuebytes
    // For variable length types we store: keyType, size, for each key: (length of key + value bytes)

    int dataType = byteBuffer.getInt();
    int size = byteBuffer.getInt();
    if (dataType == HashSetManager.SerDataType.Integer.getValue()) {
      Set set = getHashSet(FieldSpec.DataType.INT, size);
      for (int i = 0; i < size; i++) {
        set.add(byteBuffer.getInt());
      }
      return set;
    } else if (dataType == HashSetManager.SerDataType.Long.getValue()) {
      Set set = getHashSet(FieldSpec.DataType.LONG, size);
      for (int i = 0; i < size; i++) {
        set.add(byteBuffer.getLong());
      }
      return set;
    } else if (dataType == HashSetManager.SerDataType.Double.getValue()) {
      Set set = getHashSet(FieldSpec.DataType.DOUBLE, size);
      for (int i = 0; i < size; i++) {
        set.add(byteBuffer.getDouble());
      }
      return set;
    } else if (dataType == HashSetManager.SerDataType.Float.getValue()) {
      Set set = getHashSet(FieldSpec.DataType.FLOAT, size);
      for (int i = 0; i < size; i++) {
        set.add(byteBuffer.getFloat());
      }
      return set;
    } else if (dataType == HashSetManager.SerDataType.String.getValue()) {
      Set set = getHashSet(FieldSpec.DataType.STRING, size);
      for (int i = 0; i < size; i++) {
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        set.add(new String(bytes, UTF_8));
      }
      return set;
    } else {
      Set set = getHashSet(FieldSpec.DataType.BYTES, size);
      for (int i = 0; i < size; i++) {
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        set.add(bytes);
      }
      return set;
    }
  }

  /**
   * Helper function: Serializes Set of fixed length types (int,long,float,double) into a byte array.
   */
  private byte[] toBytesFixedLengthType(Set set,SerDataType keyType) {
    int size = set.size();
    byte[] bytes;

    // Besides the value bytes, we store: keyType, size
    if (keyType == SerDataType.Integer) {
      long bufferSize = (2 + (long) size) * Integer.BYTES;
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");

      bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(keyType.getValue()); // keyType of Hash Set
      byteBuffer.putInt(size); // size of HashSet

      Iterator<Integer> iterator = set.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putInt(iterator.next()); // values of Hash Set
      }
    } else if (keyType == SerDataType.Long) {
      long bufferSize = 2 * Integer.BYTES + (long) size * Long.BYTES;
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");

      bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(keyType.getValue());
      byteBuffer.putInt(size);

      Iterator<Long> iterator = set.iterator();
      while (iterator.hasNext()) {
        Long l = iterator.next();
        byteBuffer.putLong(l);
      }
    } else if (keyType == SerDataType.Float) {
      long bufferSize = 2 * Integer.BYTES + (long) size * Float.BYTES;
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");

      bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(keyType.getValue());
      byteBuffer.putInt(size);

      Iterator<Float> iterator = set.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putFloat(iterator.next());
      }
    } else { //Double
      long bufferSize = 2 * Integer.BYTES + (long) size * Double.BYTES;
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");

      bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(keyType.getValue());
      byteBuffer.putInt(size);

      Iterator<Double> iterator = set.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putDouble(iterator.next());
      }
    }
    return bytes;
  }

  /**
   * Helper function: Serializes Set<String> into a byte array.
   */
  private byte[] toBytesString(Set<String> set) {
    int size = set.size();
    // Apart from the value strings, we store the keyType, size, length of each string value
    long bufferSize = (2 + (long) size) * Integer.BYTES;
    byte[][] valueBytesArray = new byte[size][];
    int index = 0;
    Iterator<String> it = set.iterator();
    while (it.hasNext()) {
      String value = it.next();
      byte[] valueBytes = value.getBytes(UTF_8);
      bufferSize += valueBytes.length;
      valueBytesArray[index++] = valueBytes;
    }

    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
    byte[] bytes = new byte[(int) bufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(HashSetManager.SerDataType.String.getValue()); //keyType of Hash Set
    byteBuffer.putInt(size); //size of Hash Set

    for (byte[] valueBytes : valueBytesArray) {
      byteBuffer.putInt(valueBytes.length);
      byteBuffer.put(valueBytes);
    }
    return bytes;
  }

  /**
   * Serializes Set</byte[]> into a byte array.
   */
  private byte[] toBytesByteArray(Set<byte[]> set) {
    int size = set.size();
    // Besides the value bytes, we store: keyType, size, length for each value
    long bufferSize = (2 + (long) size) * Integer.BYTES;
    Iterator<byte[]> it = set.iterator();
    while (it.hasNext()) {
      byte[] value = it.next();
      bufferSize += value.length;
    }

    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
    byte[] bytes = new byte[(int) bufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(HashSetManager.SerDataType.Bytes.getValue());  //keyType of Hash Set
    byteBuffer.putInt(size);  //size of Hash Set

    it = set.iterator();
    while (it.hasNext()) {
      byte[] value = it.next();
      byteBuffer.putInt(value.length);
      byteBuffer.put(value);
    }
    return bytes;
  }

}
