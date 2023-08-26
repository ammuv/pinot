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
package org.apache.pinot.core.query.aggregation.function;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.HashSetFactory;
import org.apache.pinot.core.query.aggregation.utils.HashSetManager;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Base class for Distinct Aggregate functions that require collecting all distinct elements in a set before performing
 * the aggregate computation. This is used by DistinctSum, DistinctAvg and DistinctCount aggregation functions.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseDistinctAggregateAggregationFunction<T extends Comparable>
    extends BaseSingleInputAggregationFunction<Set, T> {
  private final AggregationFunctionType _functionType;
  private static final HashSetFactory _hashSetFactory = new HashSetFactory();

  protected BaseDistinctAggregateAggregationFunction(ExpressionContext expression,
      AggregationFunctionType aggregationFunctionType) {
    super(expression);
    _functionType = aggregationFunctionType;
  }

  @Override
  public AggregationFunctionType getType() {
    return _functionType;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public Set extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      // Use empty IntOpenHashSet as a place holder for empty result
      return new IntOpenHashSet();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (Set) result;
    }
  }

  @Override
  public Set extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      // NOTE: Return an empty IntOpenHashSet for empty result.
      return new IntOpenHashSet();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (Set) result;
    }
  }

  @Override
  public Set merge(Set intermediateResult1, Set intermediateResult2) {
    if (intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  /**
   * Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Performs aggregation for a SV column
   */
  protected void svAggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    Set valueSet = getValueSet(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        _hashSetFactory.getHasHSetManager(storedType).addInteger(valueSet, intValues);
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        _hashSetFactory.getHasHSetManager(storedType).addLong(valueSet, longValues);
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        _hashSetFactory.getHasHSetManager(storedType).addFloat(valueSet, floatValues);
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        _hashSetFactory.getHasHSetManager(storedType).addDouble(valueSet, doubleValues);
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        //noinspection ManualArrayToCollectionCopy
        _hashSetFactory.getHasHSetManager(storedType).addString(valueSet, stringValues);
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          _hashSetFactory.getHasHSetManager(storedType).addBytes(valueSet, bytesValues[i]);
          //valueSet.add(new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a MV column
   */
  protected void mvAggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        dictIdBitmap.add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    Set valueSet = getValueSet(aggregationResultHolder, storedType);
    HashSetManager hsm = _hashSetFactory.getHasHSetManager(storedType);
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          hsm.addInteger(valueSet, intValues[i]);
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          hsm.addLong(valueSet, longValues[i]);
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          hsm.addFloat(valueSet, floatValues[i]);
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          hsm.addDouble(valueSet, doubleValues[i]);
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          //noinspection ManualArrayToCollectionCopy
          //noinspection UseBulkOperation
          hsm.addString(valueSet, stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a SV column with group by on a SV column.
   */
  protected void svAggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    HashSetManager hsm = _hashSetFactory.getHasHSetManager(storedType);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT);
          hsm.addInteger(valueSet, intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG);
          hsm.addLong(valueSet, longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT);
          hsm.addFloat(valueSet, floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE);
          hsm.addDouble(valueSet, doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING);
          hsm.addString(valueSet, stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.BYTES);
          hsm.addBytes(valueSet, bytesValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a MV column with group by on a SV column.
   */
  protected void mvAggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    HashSetManager hsm = _hashSetFactory.getHasHSetManager(storedType);
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT);
          hsm.addInteger(valueSet, intValues[i]);
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG);
          hsm.addLong(valueSet, longValues[i]);
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT);
          hsm.addFloat(valueSet, floatValues[i]);
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE);
          hsm.addDouble(valueSet, doubleValues[i]);
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          Set valueSet = getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING);
          //noinspection ManualArrayToCollectionCopy
          //noinspection UseBulkOperation
          hsm.addString(valueSet, stringValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a SV column with group by on a MV column.
   */
  protected void svAggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], bytesValues[i]);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a MV column with group by on a MV column.
   */
  protected void mvAggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    HashSetManager hsm = _hashSetFactory.getHasHSetManager(storedType);
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Set intSet = getValueSet(groupByResultHolder, groupKey, DataType.INT);
            hsm.addInteger(intSet, intValues[i]);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Set longSet = getValueSet(groupByResultHolder, groupKey, DataType.LONG);
            hsm.addLong(longSet, longValues[i]);
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Set floatSet = getValueSet(groupByResultHolder, groupKey, DataType.FLOAT);
            hsm.addFloat(floatSet, floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Set doubleSet = getValueSet(groupByResultHolder, groupKey, DataType.DOUBLE);
            hsm.addDouble(doubleSet, doubleValues[i]);
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Set stringSet = getValueSet(groupByResultHolder, groupKey, DataType.STRING);
            //noinspection ManualArrayToCollectionCopy
            //noinspection UseBulkOperation
            hsm.addString(stringSet, stringValues[i]);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Returns the value set from the result holder or creates a new one if it does not exist.
   */
  protected static Set getValueSet(AggregationResultHolder aggregationResultHolder, DataType valueType) {
    Set valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = getValueSet(valueType);
      try {
        aggregationResultHolder.setValue(valueSet);
      } catch (Exception e) {
        throw new IllegalStateException("Illegal Type of value: Aggr SetFromMap error here");
      }
    }
    return valueSet;
  }

  /**
   * Helper method to create a value set for the given value type.
   */
  private static Set getValueSet(DataType valueType) {
    return _hashSetFactory.getHashSet(valueType);
  }

  private static Set getValueSet(DataType valueType, int size) {
    return _hashSetFactory.getHashSet(valueType, size);
  }

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value set for the given group key or creates a new one if it does not exist.
   */
  protected static Set getValueSet(GroupByResultHolder groupByResultHolder, int groupKey, DataType valueType) {
    Set valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = getValueSet(valueType);
      try {
        groupByResultHolder.setValueForKey(groupKey, valueSet);
      } catch (Exception e) {
        throw new IllegalStateException("Illegal Type of value: SetFromMap error here");
      }
    }
    return valueSet;
  }

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /**
   * Helper method to set INT value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      Set valueSet = getValueSet(groupByResultHolder, groupKey, DataType.INT);
      _hashSetFactory.getHasHSetManager(DataType.INT).addInteger(valueSet, value);
    }
  }

  /**
   * Helper method to set LONG value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, long value) {
    for (int groupKey : groupKeys) {
      Set valueSet = getValueSet(groupByResultHolder, groupKey, DataType.LONG);
      _hashSetFactory.getHasHSetManager(DataType.LONG).addLong(valueSet, value);
    }
  }

  /**
   * Helper method to set FLOAT value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, float value) {
    for (int groupKey : groupKeys) {
      Set valueSet = getValueSet(groupByResultHolder, groupKey, DataType.FLOAT);
      _hashSetFactory.getHasHSetManager(DataType.FLOAT).addFloat(valueSet, value);
    }
  }

  /**
   * Helper method to set DOUBLE value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, double value) {
    for (int groupKey : groupKeys) {
      Set valueSet = getValueSet(groupByResultHolder, groupKey, DataType.DOUBLE);
      _hashSetFactory.getHasHSetManager(DataType.DOUBLE).addDouble(valueSet, value);
    }
  }

  /**
   * Helper method to set STRING value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, String value) {
    for (int groupKey : groupKeys) {
      Set valueSet = getValueSet(groupByResultHolder, groupKey, DataType.STRING);
      _hashSetFactory.getHasHSetManager(DataType.STRING).addString(valueSet, value);
    }
  }

  /**
   * Helper method to set BYTES value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, byte[] value) {
    for (int groupKey : groupKeys) {
      Set valueSet = getValueSet(groupByResultHolder, groupKey, DataType.BYTES);
      _hashSetFactory.getHasHSetManager(DataType.BYTES).addBytes(valueSet, value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to values for dictionary-encoded expression.
   */
  private static Set convertToValueSet(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    int numValues = dictIdBitmap.getCardinality();
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType storedType = dictionary.getValueType();
    Set valueSet = getValueSet(storedType, numValues);
    HashSetManager hsm = _hashSetFactory.getHasHSetManager(storedType);
    switch (storedType) {
      case INT:
        while (iterator.hasNext()) {
          hsm.addInteger(valueSet, dictionary.getIntValue(iterator.next()));
        }
        return valueSet;
      case LONG:
        while (iterator.hasNext()) {
          hsm.addLong(valueSet, dictionary.getLongValue(iterator.next()));
        }
        return valueSet;
      case FLOAT:
        while (iterator.hasNext()) {
          hsm.addFloat(valueSet, dictionary.getFloatValue(iterator.next()));
        }
        return valueSet;
      case DOUBLE:
        while (iterator.hasNext()) {
          hsm.addDouble(valueSet, dictionary.getDoubleValue(iterator.next()));
        }
        return valueSet;
      case STRING:
        while (iterator.hasNext()) {
          hsm.addString(valueSet, dictionary.getStringValue(iterator.next()));
        }
        return valueSet;
      case BYTES:
        while (iterator.hasNext()) {
          hsm.addBytes(valueSet, dictionary.getBytesValue(iterator.next()));
        }
        return valueSet;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function: " + storedType);
    }
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new RoaringBitmap();
    }
  }
}
