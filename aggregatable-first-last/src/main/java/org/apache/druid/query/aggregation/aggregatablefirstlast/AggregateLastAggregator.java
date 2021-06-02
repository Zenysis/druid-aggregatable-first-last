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
import org.apache.druid.segment.BaseLongColumnValueSelector;

public class AggregateLastAggregator extends FirstLastAggregator
{
  long lastTime;

  public AggregateLastAggregator(
      final BaseLongColumnValueSelector timeSelector,
      final AggregatorCreator aggregatorCreator
  )
  {
    super(timeSelector, aggregatorCreator);
    lastTime = Long.MIN_VALUE;
  }

  @Override
  public Object get()
  {
    return new SerializablePair<>(lastTime, aggregator.get());
  }

  @Override
  public void aggregate()
  {
    long time = timeSelector.getLong();
    if (time >= lastTime) {
      if (time > lastTime) {
        lastTime = time;
        resetCurrentValue();
      }
      aggregateCurrentValue();
    }
  }
}
