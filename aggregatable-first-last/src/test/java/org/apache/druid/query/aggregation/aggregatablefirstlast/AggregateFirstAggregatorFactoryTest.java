/*
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

package org.apache.druid.query.aggregation.aggregatablefirstlast;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class AggregateFirstAggregatorFactoryTest
{
  private void aggregate(
      AggregateFirstAggregator agg,
      TestDoubleColumnSelectorImpl valueSelector,
      TestLongColumnSelector timeSelector
  )
  {
    agg.aggregate();
    valueSelector.increment();
    timeSelector.increment();
  }

  private SerializablePair<Long, Double> combine(
      AggregateFirstAggregatorFactory factory,
      Aggregator agg1,
      Aggregator agg2
  )
  {
    return (SerializablePair<Long, Double>) factory.combine(agg1.get(), agg2.get());
  }

  private SerializablePair<Long, Double> getPair(AggregateFirstAggregator agg)
  {
    return (SerializablePair<Long, Double>) agg.get();
  }

  @Test
  public void testCombineDoubleSumSameTime()
  {
    final double[] values = {0.15d, 0.27d, 0.48d};
    final long[] times = {4567L, 4567L, 4567L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateFirstAggregatorFactory factory = new AggregateFirstAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        null
    );
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        valueSelector,
        ValueType.DOUBLE,
        timeSelector
    );
    AggregateFirstAggregator agg1 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    AggregateFirstAggregator agg2 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    // Prime the aggregators to have values.
    double expectedFirst1 = values[0];
    double expectedSecond1 = values[0] + values[1];
    assertValues(
        agg1,
        valueSelector,
        timeSelector,
        NullHandling.defaultDoubleValue(),
        expectedFirst1,
        expectedSecond1
    );

    double expectedFirst2 = values[2];
    assertValues(
        agg2,
        valueSelector,
        timeSelector,
        NullHandling.defaultDoubleValue(),
        expectedFirst2
    );
    SerializablePair<Long, Double> combined = combine(factory, agg1, agg2);

    long expectedTime = times[0];
    double expectedValue = values[0] + values[1] + values[2];
    assertCombined(combined, expectedTime, expectedValue);
  }

  @Test
  public void testCombineDoubleSumDifferentTime()
  {
    final double[] values = {0.15d, 0.27d, 0.48d};
    final long[] times = {4567L, 5678L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateFirstAggregatorFactory factory = new AggregateFirstAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        null
    );
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        valueSelector,
        ValueType.DOUBLE,
        timeSelector
    );
    AggregateFirstAggregator agg1 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    AggregateFirstAggregator agg2 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    // Prime the aggregators to have values.
    double expectedFirst1 = values[0];
    double expectedSecond1 = values[0];
    assertValues(
        agg1,
        valueSelector,
        timeSelector,
        NullHandling.defaultDoubleValue(),
        expectedFirst1,
        expectedSecond1
    );

    double expectedFirst2 = values[2];
    assertValues(
        agg2,
        valueSelector,
        timeSelector,
        NullHandling.defaultDoubleValue(),
        expectedFirst2
    );
    SerializablePair<Long, Double> combined = combine(factory, agg1, agg2);

    long expectedTime = times[0];
    double expectedValue = values[0];
    assertCombined(combined, expectedTime, expectedValue);
  }

  @Test
  public void testCombineDoubleSumValueReset()
  {
    final double[] values = {0.15d, 0.27d, 0.48d, 0.89d};
    final long[] times = {4567L, 4567L, 6789L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateFirstAggregatorFactory factory = new AggregateFirstAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        null
    );
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        valueSelector,
        ValueType.DOUBLE,
        timeSelector
    );
    AggregateFirstAggregator agg1 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    AggregateFirstAggregator agg2 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    // Prime the aggregators to have values.
    double expectedFirst1 = values[0];
    double expectedSecond1 = values[0] + values[1];
    assertValues(
        agg1,
        valueSelector,
        timeSelector,
        NullHandling.defaultDoubleValue(),
        expectedFirst1,
        expectedSecond1
    );

    double expectedFirst2 = values[2];
    double expectedSecond2 = values[2] + values[3];
    assertValues(
        agg2,
        valueSelector,
        timeSelector,
        NullHandling.defaultDoubleValue(),
        expectedFirst2,
        expectedSecond2
    );
    SerializablePair<Long, Double> combined = combine(factory, agg1, agg2);

    long expectedTime = times[0];
    double expectedValue = values[0] + values[1];
    assertCombined(combined, expectedTime, expectedValue);
  }

  @Test
  public void testCombineDoubleMaxSameTime()
  {
    final double[] values = {0.15d, 0.48d, 0.27d};
    final long[] times = {4567L, 4567L, 4567L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateFirstAggregatorFactory factory = new AggregateFirstAggregatorFactory(
        new DoubleMaxAggregatorFactory("billy", "value"),
        null
    );
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        valueSelector,
        ValueType.DOUBLE,
        timeSelector
    );
    AggregateFirstAggregator agg1 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    AggregateFirstAggregator agg2 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    // Prime the aggregators to have values.
    double expectedFirst1 = values[0];
    double expectedSecond1 = values[1];
    assertValues(
        agg1,
        valueSelector,
        timeSelector,
        Double.NEGATIVE_INFINITY,
        expectedFirst1,
        expectedSecond1
    );

    double expectedFirst2 = values[2];
    assertValues(
        agg2,
        valueSelector,
        timeSelector,
        Double.NEGATIVE_INFINITY,
        expectedFirst2
    );
    SerializablePair<Long, Double> combined = combine(factory, agg1, agg2);

    long expectedTime = times[0];
    double expectedValue = values[1];
    assertCombined(combined, expectedTime, expectedValue);
  }

  @Test
  public void testCombineDoubleMaxDifferentTime()
  {
    final double[] values = {0.15d, 0.48d, 0.27d};
    final long[] times = {4567L, 5678L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateFirstAggregatorFactory factory = new AggregateFirstAggregatorFactory(
        new DoubleMaxAggregatorFactory("billy", "value"),
        null
    );
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        valueSelector,
        ValueType.DOUBLE,
        timeSelector
    );
    AggregateFirstAggregator agg1 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    AggregateFirstAggregator agg2 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    // Prime the aggregators to have values.
    double expectedFirst1 = values[0];
    double expectedSecond1 = values[0];
    assertValues(
        agg1,
        valueSelector,
        timeSelector,
        Double.NEGATIVE_INFINITY,
        expectedFirst1,
        expectedSecond1
    );

    double expectedFirst2 = values[2];
    assertValues(
        agg2,
        valueSelector,
        timeSelector,
        Double.NEGATIVE_INFINITY,
        expectedFirst2
    );
    SerializablePair<Long, Double> combined = combine(factory, agg1, agg2);

    long expectedTime = times[0];
    double expectedValue = values[0];
    assertCombined(combined, expectedTime, expectedValue);
  }

  @Test
  public void testCombineDoubleMaxValueReset()
  {
    final double[] values = {0.15d, 0.27d, 0.18d, 0.09d};
    final long[] times = {4567L, 4567L, 6789L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateFirstAggregatorFactory factory = new AggregateFirstAggregatorFactory(
        new DoubleMaxAggregatorFactory("billy", "value"),
        null
    );
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        valueSelector,
        ValueType.DOUBLE,
        timeSelector
    );
    AggregateFirstAggregator agg1 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    AggregateFirstAggregator agg2 = (AggregateFirstAggregator) factory.factorize(
        columnSelector
    );
    // Prime the aggregators to have values.
    double expectedFirst1 = values[0];
    double expectedSecond1 = values[1];
    assertValues(
        agg1,
        valueSelector,
        timeSelector,
        Double.NEGATIVE_INFINITY,
        expectedFirst1,
        expectedSecond1
    );

    double expectedFirst2 = values[2];
    double expectedSecond2 = values[2];
    assertValues(
        agg2,
        valueSelector,
        timeSelector,
        Double.NEGATIVE_INFINITY,
        expectedFirst2,
        expectedSecond2
    );
    SerializablePair<Long, Double> combined = combine(factory, agg1, agg2);

    long expectedTime = times[0];
    double expectedValue = values[1];
    assertCombined(combined, expectedTime, expectedValue);
  }

  private void assertValues(
      AggregateFirstAggregator agg,
      TestDoubleColumnSelectorImpl valueSelector,
      TestLongColumnSelector timeSelector,
      Double initialValue,
      double... expectedVals
  )
  {
    assertValue(agg, initialValue);
    for (double expectedVal : expectedVals) {
      aggregate(agg, valueSelector, timeSelector);
      assertValue(agg, expectedVal);
    }
  }

  private void assertValue(AggregateFirstAggregator agg, Double expectedValue)
  {
    Assert.assertEquals(expectedValue, getPair(agg).rhs);
    Assert.assertEquals(expectedValue, getPair(agg).rhs);
    Assert.assertEquals(expectedValue, getPair(agg).rhs);
    Assert.assertEquals(expectedValue, agg.getDouble(), 0.0001);
    Assert.assertEquals(expectedValue, agg.getDouble(), 0.0001);
    Assert.assertEquals(expectedValue, agg.getDouble(), 0.0001);
  }

  private void assertCombined(
      SerializablePair<Long, Double> combined,
      Long expectedTime,
      Double expectedValue
  )
  {
    Assert.assertEquals(expectedTime, combined.lhs);
    Assert.assertEquals(expectedValue, combined.rhs);
  }

  private ColumnSelectorFactory makeColumnSelector(
      final ColumnValueSelector<?> valueSelector,
      final ValueType valueType,
      final TestLongColumnSelector timeSelector
  )
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
      {
        if ("value".equals(columnName)) {
          return valueSelector;
        }
        if (columnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {
          return timeSelector;
        }
        throw new UnsupportedOperationException();
      }

      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        ColumnCapabilitiesImpl caps;
        if ("value".equals(columnName)) {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(valueType);
          caps.setDictionaryEncoded(false);
          caps.setHasBitmapIndexes(false);
        } else if (columnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(ValueType.LONG);
          caps.setDictionaryEncoded(false);
          caps.setHasBitmapIndexes(false);
        } else {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(ValueType.STRING);
          caps.setDictionaryEncoded(true);
          caps.setHasBitmapIndexes(true);
        }
        return caps;
      }
    };
  }
}
