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
import org.apache.druid.query.aggregation.BufferAggregator;
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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class AggregateLastBufferAggregatorTest extends InitializedNullHandlingTest
{
  private void aggregate(
      BufferAggregator agg,
      TestDoubleColumnSelectorImpl valueSelector,
      TestLongColumnSelector timeSelector,
      ByteBuffer buffer,
      int position
  )
  {
    agg.aggregate(buffer, position);
    valueSelector.increment();
    timeSelector.increment();
  }

  private SerializablePair<Long, Double> getPair(
      BufferAggregator agg,
      ByteBuffer buffer,
      int position
  )
  {
    return (SerializablePair<Long, Double>) agg.get(buffer, position);
  }

  @Test
  public void testAggregateDoubleSumSameTime()
  {
    final double[] values = {0.15d, 0.27d, 0.48d};
    final long[] times = {4567L, 4567L, 4567L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateLastAggregatorFactory factory = new AggregateLastAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        null
    );
    BufferAggregator agg = factory.factorizeBuffered(
        makeColumnSelector(valueSelector, ValueType.DOUBLE, timeSelector)
    );
    ByteBuffer buffer = ByteBuffer.wrap(new byte[factory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    double expectedFirst = values[0];
    double expectedSecond = values[0] + values[1];
    double expectedThird = values[0] + values[1] + values[2];
    assertValues(
        agg,
        valueSelector,
        timeSelector,
        buffer,
        0,
        NullHandling.defaultDoubleValue(),
        expectedFirst,
        expectedSecond,
        expectedThird
    );
  }

  @Test
  public void testAggregateDoubleSumDifferentTime()
  {
    final double[] values = {0.15d, 0.27d, 0.48d};
    final long[] times = {4567L, 5678L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateLastAggregatorFactory factory = new AggregateLastAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        null
    );
    BufferAggregator agg = factory.factorizeBuffered(
        makeColumnSelector(valueSelector, ValueType.DOUBLE, timeSelector)
    );
    ByteBuffer buffer = ByteBuffer.wrap(new byte[factory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    double expectedFirst = values[0];
    double expectedSecond = values[1];
    double expectedThird = values[2];
    assertValues(
        agg,
        valueSelector,
        timeSelector,
        buffer,
        0,
        NullHandling.defaultDoubleValue(),
        expectedFirst,
        expectedSecond,
        expectedThird
    );
  }

  @Test
  public void testAggregateDoubleSumValueReset()
  {
    final double[] values = {0.15d, 0.27d, 0.48d, 0.89d};
    final long[] times = {4567L, 4567L, 6789L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateLastAggregatorFactory factory = new AggregateLastAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        null
    );
    BufferAggregator agg = factory.factorizeBuffered(
        makeColumnSelector(valueSelector, ValueType.DOUBLE, timeSelector)
    );
    ByteBuffer buffer = ByteBuffer.wrap(new byte[factory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    double expectedFirst = values[0];
    double expectedSecond = values[0] + values[1];
    double expectedThird = values[2];
    double expectedFourth = values[2] + values[3];
    assertValues(
        agg,
        valueSelector,
        timeSelector,
        buffer,
        0,
        NullHandling.defaultDoubleValue(),
        expectedFirst,
        expectedSecond,
        expectedThird,
        expectedFourth
    );
  }

  @Test
  public void testAggregateDoubleMaxSameTime()
  {
    final double[] values = {0.15d, 0.48d, 0.27d};
    final long[] times = {4567L, 4567L, 4567L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateLastAggregatorFactory factory = new AggregateLastAggregatorFactory(
        new DoubleMaxAggregatorFactory("billy", "value"),
        null
    );
    BufferAggregator agg = factory.factorizeBuffered(
        makeColumnSelector(valueSelector, ValueType.DOUBLE, timeSelector)
    );
    ByteBuffer buffer = ByteBuffer.wrap(new byte[factory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    double expectedFirst = values[0];
    double expectedSecond = values[1];
    double expectedThird = values[1];
    assertValues(
        agg,
        valueSelector,
        timeSelector,
        buffer,
        0,
        Double.NEGATIVE_INFINITY,
        expectedFirst,
        expectedSecond,
        expectedThird
    );
  }

  @Test
  public void testAggregateDoubleMaxDifferentTime()
  {
    final double[] values = {0.15d, 0.48d, 0.27d};
    final long[] times = {4567L, 5678L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateLastAggregatorFactory factory = new AggregateLastAggregatorFactory(
        new DoubleMaxAggregatorFactory("billy", "value"),
        null
    );
    BufferAggregator agg = factory.factorizeBuffered(
        makeColumnSelector(valueSelector, ValueType.DOUBLE, timeSelector)
    );
    ByteBuffer buffer = ByteBuffer.wrap(new byte[factory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    double expectedFirst = values[0];
    double expectedSecond = values[1];
    double expectedThird = values[2];
    assertValues(
        agg,
        valueSelector,
        timeSelector,
        buffer,
        0,
        Double.NEGATIVE_INFINITY,
        expectedFirst,
        expectedSecond,
        expectedThird
    );
  }

  @Test
  public void testAggregateDoubleMaxValueReset()
  {
    final double[] values = {0.15d, 0.27d, 0.18d, 0.09d};
    final long[] times = {4567L, 4567L, 6789L, 6789L};
    final TestDoubleColumnSelectorImpl valueSelector = new TestDoubleColumnSelectorImpl(values);
    final TestLongColumnSelector timeSelector = new TestLongColumnSelector(times);
    AggregateLastAggregatorFactory factory = new AggregateLastAggregatorFactory(
        new DoubleMaxAggregatorFactory("billy", "value"),
        null
    );
    BufferAggregator agg = factory.factorizeBuffered(
        makeColumnSelector(valueSelector, ValueType.DOUBLE, timeSelector)
    );
    ByteBuffer buffer = ByteBuffer.wrap(new byte[factory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    double expectedFirst = values[0];
    double expectedSecond = values[1];
    double expectedThird = values[2];
    double expectedFourth = values[2];
    assertValues(
        agg,
        valueSelector,
        timeSelector,
        buffer,
        0,
        Double.NEGATIVE_INFINITY,
        expectedFirst,
        expectedSecond,
        expectedThird,
        expectedFourth
    );
  }

  private void assertValues(
      BufferAggregator agg,
      TestDoubleColumnSelectorImpl valueSelector,
      TestLongColumnSelector timeSelector,
      ByteBuffer buffer,
      int position,
      Double initialValue,
      double... expectedVals
  )
  {
    assertValue(agg, buffer, position, initialValue);
    for (double expectedVal : expectedVals) {
      aggregate(agg, valueSelector, timeSelector, buffer, position);
      assertValue(agg, buffer, position, expectedVal);
    }
  }

  private void assertValue(
      BufferAggregator agg,
      ByteBuffer buffer,
      int position,
      Double expectedValue
  )
  {
    Assert.assertEquals(expectedValue, getPair(agg, buffer, position).rhs);
    Assert.assertEquals(expectedValue, getPair(agg, buffer, position).rhs);
    Assert.assertEquals(expectedValue, getPair(agg, buffer, position).rhs);
    Assert.assertEquals(expectedValue, agg.getDouble(buffer, position), 0.0001);
    Assert.assertEquals(expectedValue, agg.getDouble(buffer, position), 0.0001);
    Assert.assertEquals(expectedValue, agg.getDouble(buffer, position), 0.0001);
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
