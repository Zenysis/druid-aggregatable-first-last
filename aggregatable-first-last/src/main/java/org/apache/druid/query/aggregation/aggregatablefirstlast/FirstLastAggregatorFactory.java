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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class FirstLastAggregatorFactory extends AggregatorFactory
{
  final AggregatorFactory delegate;

  @Nullable
  final String name;

  @JsonCreator
  public FirstLastAggregatorFactory(
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("name") @Nullable String name
  )
  {
    Preconditions.checkNotNull(delegate, "Must have a valid, non-null aggregator");
    this.delegate = delegate;
    this.name = name;
  }

  @Override
  public boolean canVectorize()
  {
    return delegate.canVectorize();
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }

    SerializablePair<Long, Object> lhsPair = (SerializablePair<Long, Object>) lhs;
    SerializablePair<Long, Object> rhsPair = (SerializablePair<Long, Object>) rhs;
    if (lhsPair.lhs.equals(rhsPair.lhs)) {
      return new SerializablePair<>(lhsPair.lhs, delegate.combine(lhsPair.rhs, rhsPair.rhs));
    }
    return combineNonEqual(lhsPair, rhsPair);
  }

  abstract Object combineNonEqual(
      SerializablePair<Long, Object> lhsPair,
      SerializablePair<Long, Object> rhsPair
  );

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("FirstLastAggregatorFactory is not supported during ingestion for rollup");
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), map.get("rhs"));
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return delegate.finalizeComputation(((SerializablePair<Long, Object>) object).rhs);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    String name = this.name;
    if (Strings.isNullOrEmpty(name)) {
      name = delegate.getName();
    }
    return name;
  }

  @JsonProperty
  public AggregatorFactory getAggregator()
  {
    return delegate;
  }

  @Override
  public List<String> requiredFields()
  {
    final List<String> delegateFields = delegate.requiredFields();
    final List<String> requiredFields = new ArrayList<>(delegateFields.size() + 1);
    requiredFields.addAll(delegateFields);
    requiredFields.add(ColumnHolder.TIME_COLUMN_NAME);
    return requiredFields;
  }

  @Override
  public String getTypeName()
  {
    return delegate.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // Timestamp + aggregator size
    return Long.BYTES + delegate.getMaxIntermediateSizeWithNulls();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return delegate.getRequiredColumns();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FirstLastAggregatorFactory that = (FirstLastAggregatorFactory) o;
    return Objects.equals(delegate, that.delegate) &&
           Objects.equals(name, that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(delegate, name);
  }
}
