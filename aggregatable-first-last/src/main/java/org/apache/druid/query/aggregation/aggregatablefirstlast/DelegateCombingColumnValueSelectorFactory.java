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

import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;

/**
 * Wrap the ColumnSelectorFactory that is used during the `combining` stage of
 * aggregation. The intermediary value stored for the FirstLastAggregatorFactory
 * in the original ColumnSelectorFactory is the type produced by `.combine()`.
 * The nested aggregator needs to work as if its value was in a separate column,
 * not as part of the SerializablePair<Long, Object>. This class presents
 * column value selectors to the FirstLastAggregators that act like it is the
 * first stage of aggregation: allowing access to the Time and nested agg
 * columns separately.
 */
public class DelegateCombingColumnValueSelectorFactory implements ColumnSelectorFactory
{
  final ColumnSelectorFactory originalColumnSelectorFactory;
  final ColumnValueSelector<SerializablePair<Long, Object>> selector;
  final String delegateName;

  public DelegateCombingColumnValueSelectorFactory(
      ColumnSelectorFactory originalColumnSelectorFactory,
      ColumnValueSelector<SerializablePair<Long, Object>> selector,
      String delegateName
  )
  {
    Preconditions.checkNotNull(delegateName, "Delegate name cannot be null");
    this.originalColumnSelectorFactory = originalColumnSelectorFactory;
    this.selector = selector;
    this.delegateName = delegateName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return originalColumnSelectorFactory.makeDimensionSelector(dimensionSpec);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (this.delegateName.equals(columnName)) {
      return makeWrappedColumnValueSelector(false);
    }

    if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
      return makeWrappedColumnValueSelector(true);
    }

    return originalColumnSelectorFactory.makeColumnValueSelector(columnName);
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return originalColumnSelectorFactory.getColumnCapabilities(columnName);
  }

  private ColumnValueSelector<?> makeWrappedColumnValueSelector(boolean isTime)
  {
    return new ColumnValueSelector<Object>()
    {
      private Object getValue(boolean replaceNullWithDefault)
      {
        if (selector.isNull()) {
          return replaceNullWithDefault ? 0 : null;
        }

        SerializablePair<Long, Object> pair = selector.getObject();
        return isTime ? pair.lhs : pair.rhs;
      }

      @Override
      public double getDouble()
      {
        return (double) getValue(NullHandling.replaceWithDefault());
      }

      @Override
      public float getFloat()
      {
        return (float) getValue(NullHandling.replaceWithDefault());
      }

      @Override
      public long getLong()
      {
        return (long) getValue(NullHandling.replaceWithDefault());
      }

      @Override
      public Object getObject()
      {
        return getValue(false);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Nothing to inspect.
      }

      @Override
      public boolean isNull()
      {
        return getValue(false) == null;
      }

      @Override
      public Class<Object> classOfObject()
      {
        return Object.class;
      }
    };
  }
}
