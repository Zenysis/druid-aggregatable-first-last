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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;

public class AggregateLastBufferAggregator extends FirstLastBufferAggregator
{
  public AggregateLastBufferAggregator(
      final BaseLongColumnValueSelector timeSelector,
      final BufferAggregator delegate
  )
  {
    super(timeSelector, delegate);
  }

  @Override
  public Long getInitialTimestamp()
  {
    return Long.MIN_VALUE;
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    long time = timeSelector.getLong();
    long lastTime = buf.getLong(position);
    if (time >= lastTime) {
      if (time > lastTime) {
        buf.putLong(position, time);

        // Clear the existing value since this timestamp is larger.
        delegate.init(buf, position + VALUE_OFFSET);
      }
      delegate.aggregate(buf, position + VALUE_OFFSET);
    }
  }
}
