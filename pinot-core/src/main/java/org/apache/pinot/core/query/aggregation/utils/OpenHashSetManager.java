package org.apache.pinot.core.query.aggregation.utils;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Set;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * HashSetManager for OpenHashSet Serialization and Deserialization not written since Pinot already supports it
 */
public class OpenHashSetManager implements HashSetManager {

  @Override
  public void addInteger(Set set, int value) {
    IntOpenHashSet intSet = (IntOpenHashSet) set;
    intSet.add(value);
  }

  @Override
  public void addFloat(Set set, float value) {
    FloatOpenHashSet floatSet = (FloatOpenHashSet) set;
    floatSet.add(value);
  }

  @Override
  public void addDouble(Set set, double value) {
    DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) set;
    doubleSet.add(value);
  }

  @Override
  public void addLong(Set set, long value) {
    LongOpenHashSet longSet = (LongOpenHashSet) set;
    longSet.add(value);
  }

  @Override
  public void addString(Set set, String value) {
    ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) set;
    stringSet.add(value);
  }

  @Override
  public void addInteger(Set set, int[] values) {
    IntOpenHashSet intSet = (IntOpenHashSet) set;
    for (int value : values) {
      intSet.add(value);
    }
  }

  @Override
  public void addFloat(Set set, float[] values) {
    FloatOpenHashSet floatSet = (FloatOpenHashSet) set;
    for (float value : values) {
      floatSet.add(value);
    }
  }

  @Override
  public void addDouble(Set set, double[] values) {
    DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) set;
    for (double value : values) {
      doubleSet.add(value);
    }
  }

  @Override
  public void addLong(Set set, long[] values) {
    LongOpenHashSet longSet = (LongOpenHashSet) set;
    for (long value : values) {
      longSet.add(value);
    }
  }

  @Override
  public void addString(Set set, String[] values) {
    ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) set;
    for (String value : values) {
      stringSet.add(value);
    }
  }

  @Override
  public void addBytes(Set set, byte[] value) {
    ObjectOpenHashSet<ByteArray> byteSet = (ObjectOpenHashSet<ByteArray>) set;
    byteSet.add(new ByteArray(value));
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType) {
    switch (valueType) {
      case INT:
        return new IntOpenHashSet();
      case LONG:
        return new LongOpenHashSet();
      case FLOAT:
        return new FloatOpenHashSet();
      case DOUBLE:
        return new DoubleOpenHashSet();
      case STRING:
      case BYTES:
        return new ObjectOpenHashSet();
      default:
        throw new IllegalStateException("Illegal data type for getOpenHashSet " + valueType);
    }
  }

  @Override
  public Set getHashSet(FieldSpec.DataType valueType, int size) {
    switch (valueType) {
      case INT:
        return new IntOpenHashSet(size);
      case LONG:
        return new LongOpenHashSet(size);
      case FLOAT:
        return new FloatOpenHashSet(size);
      case DOUBLE:
        return new DoubleOpenHashSet(size);
      case STRING:
      case BYTES:
        return new ObjectOpenHashSet(size);
      default:
        throw new IllegalStateException("Illegal data type for getOpenHashSet " + valueType);
    }
  }

  @Override
  public Set<Record> getRecordHashSet(int size) {
    return new ObjectOpenHashSet<Record>(size); //We use ObjectOpenHashSet
  }
}
