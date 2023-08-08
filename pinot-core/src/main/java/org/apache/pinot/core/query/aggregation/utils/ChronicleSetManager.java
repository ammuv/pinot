package org.apache.pinot.core.query.aggregation.utils;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.data.FieldSpec;

import static java.nio.charset.StandardCharsets.UTF_8;


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

}
