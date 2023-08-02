package org.apache.pinot.core.query.aggregation.utils;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.HashMap;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import net.openhft.chronicle.set.ChronicleSetBuilder;

public class HashSetFactory{
  public HashMap<FieldSpec.DataType,String> _typeToSetMap; //Map for which default hashSet to use for each type
  int _defaultSizeChronicleSet = 5000;

  //todo add functions to allow change of map values

  //default construct uses ChronicleSet for fixed size data types and OpenHashSet for others
  public HashSetFactory(){
    _typeToSetMap = new HashMap<FieldSpec.DataType, String>(6);
    _typeToSetMap.put(FieldSpec.DataType.INT,"ChronicleSet");
    _typeToSetMap.put(FieldSpec.DataType.LONG,"OpenHashSet");
    _typeToSetMap.put(FieldSpec.DataType.FLOAT,"OpenHashSet");
    _typeToSetMap.put(FieldSpec.DataType.DOUBLE,"OpenHashSet");
    _typeToSetMap.put(FieldSpec.DataType.STRING,"OpenHashSet");
    _typeToSetMap.put(FieldSpec.DataType.BYTES,"OpenHashSet");
  }

  public Set getHashSet(FieldSpec.DataType valueType){
    if(_typeToSetMap.containsKey(valueType)){
      if(_typeToSetMap.get(valueType)=="ChronicleSet"){
        return getChronicleHashSet(valueType,_defaultSizeChronicleSet);
      }
      else{
        return getOpenHashSet(valueType);
      }
    }
    else{
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function Hash Set");
    }
  }

  public Set getHashSet(FieldSpec.DataType valueType, int size){
    if(_typeToSetMap.containsKey(valueType)){
      if(_typeToSetMap.get(valueType)=="ChronicleSet"){
        return getChronicleHashSet(valueType,_defaultSizeChronicleSet);
      }
      else{
        return getOpenHashSet(valueType,size);
      }
    }
    else{
      throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function Hash Set");
    }
  }

  Set getChronicleHashSet(FieldSpec.DataType valueType,int size) {
    switch (valueType) {
      case INT:
        return ChronicleSetBuilder.of(Integer.class).entries(size).create();
      case LONG:
        return ChronicleSetBuilder.of(Long.class).entries(size).create();
      case FLOAT:
        return ChronicleSetBuilder.of(Float.class).entries(size).create();
      case DOUBLE:
        return ChronicleSetBuilder.of(Double.class).entries(size).create();
      case STRING:
      case BYTES:
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function Chronicle Hash Set - we use it only for fixed length data types");
    }
  }

  Set getOpenHashSet(FieldSpec.DataType valueType,int size) {
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
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function getOpenHashSet");
    }
  }

  Set getOpenHashSet(FieldSpec.DataType valueType) {
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
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function getOpenHashSet");
    }
  }
}
