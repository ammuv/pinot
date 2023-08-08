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
    OpenHashSet, ChronicleSet, MapDBSet
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

    _typeToSetMap.put(FieldSpec.DataType.INT, m);
    _typeToSetMap.put(FieldSpec.DataType.LONG, m);
    _typeToSetMap.put(FieldSpec.DataType.FLOAT, m);
    _typeToSetMap.put(FieldSpec.DataType.DOUBLE, m);
    _typeToSetMap.put(FieldSpec.DataType.STRING, o);
    _typeToSetMap.put(FieldSpec.DataType.BYTES, o);

    _hashSetManagerMap = new HashMap<>(3);
    _hashSetManagerMap.put(HashSetType.ChronicleSet, new ChronicleSetManager());
    _hashSetManagerMap.put(HashSetType.OpenHashSet, new OpenHashSetManager());
    _hashSetManagerMap.put(HashSetType.MapDBSet, new MapDBSetManager());
    _hashSetTypeForRecord = HashSetType.ChronicleSet;
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
      return hm.getHashSet(valueType,size);
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
   *  Returns a new hashSet with keys as Records of a specified size
   *  If the parameter isOpenHashSet is true, it returns OpenHashSet
   *  Otherwise it returns the HashSet according to the configured _hashSetTypeForRecord
   */
  public Set<Record> getRecordHashSet(boolean isOpenHashSet,int size){
    if(isOpenHashSet)
      return _hashSetManagerMap.get(HashSetType.OpenHashSet).getRecordHashSet(size);
    else
      return _hashSetManagerMap.get(_hashSetTypeForRecord).getRecordHashSet(size);
  }

}
