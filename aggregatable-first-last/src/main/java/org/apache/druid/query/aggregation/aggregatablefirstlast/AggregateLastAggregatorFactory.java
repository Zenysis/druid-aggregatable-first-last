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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Run the provided aggregation over the values with the largest corresponding
 * timestamp.
 */
@JsonTypeName("aggregateFirst")
public class AggregateLastAggregatorFactory extends FirstLastAggregatorFactory
{
  public static final byte CACHE_TYPE_ID = 0x79;

  public AggregateLastAggregatorFactory(
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("name") @Nullable String name
  )
  {
    super(delegate, name);
  }

  @Override
  protected FirstLastAggregator buildAggregator(
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    return new AggregateLastAggregator(
        columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
        () -> delegate.factorize(columnSelectorFactory)
    );
  }

  @Override
  protected FirstLastBufferAggregator buildBufferAggregator(
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    return new AggregateLastBufferAggregator(
        columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
        delegate.factorizeBuffered(columnSelectorFactory)
    );
  }

  @Override
  public Object combineNonEqual(
      SerializablePair<Long, Object> lhsPair,
      SerializablePair<Long, Object> rhsPair
  )
  {
    if (lhsPair.lhs > rhsPair.lhs) {
      return lhsPair;
    }
    return rhsPair;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new AggregateLastAggregatorFactory(delegate.getCombiningFactory(), name)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
      {
        return buildAggregator(
            makeCombiningColumnSelectorFactory(columnSelectorFactory)
        );
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
      {
        return buildBufferAggregator(
            makeCombiningColumnSelectorFactory(columnSelectorFactory)
        );
      }
    };
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] aggregatorCacheKey = delegate.getCacheKey();
    return ByteBuffer.allocate(1 + aggregatorCacheKey.length)
                     .put(CACHE_TYPE_ID)
                     .put(aggregatorCacheKey)
                     .array();
  }

  @Override
  public String toString()
  {
    return "AggregateLastAggregatorFactory{" +
           "delegate=" + delegate +
           ", name='" + name + '\'' +
           '}';
  }
}
