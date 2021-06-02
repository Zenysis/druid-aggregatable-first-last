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

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

/**
 * Base type for aggregating first/last values when the timestamp is the same.
 * The child aggregator is stored on-heap.
 */
public abstract class FirstLastAggregator implements Aggregator
{
  final BaseLongColumnValueSelector timeSelector;
  private final AggregatorCreator aggregatorCreator;

  Aggregator aggregator;

  public FirstLastAggregator(
      final BaseLongColumnValueSelector timeSelector,
      final AggregatorCreator aggregatorCreator
  )
  {
    this.timeSelector = timeSelector;
    this.aggregatorCreator = aggregatorCreator;
    this.aggregator = aggregatorCreator.create();
  }

  @Override
  public float getFloat()
  {
    return aggregator.getFloat();
  }

  @Override
  public long getLong()
  {
    return aggregator.getLong();
  }

  @Override
  public double getDouble()
  {
    return aggregator.getDouble();
  }

  @Override
  public void close()
  {
    aggregator.close();
  }

  /**
   * Replace the stored value with the current value of the aggregator.
   */
  public void resetCurrentValue()
  {
    aggregator.close();
    aggregator = aggregatorCreator.create();
  }

  /**
   * Update the stored aggregator's value by including the current value of
   * the aggregator.
   */
  public void aggregateCurrentValue()
  {
    aggregator.aggregate();
  }
}
