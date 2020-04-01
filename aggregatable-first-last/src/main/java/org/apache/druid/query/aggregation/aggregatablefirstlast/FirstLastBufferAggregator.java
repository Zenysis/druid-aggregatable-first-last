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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;

public abstract class FirstLastBufferAggregator implements BufferAggregator
{
  final BaseLongColumnValueSelector timeSelector;
  final BufferAggregator delegate;
  static final int VALUE_OFFSET = Long.BYTES;

  public FirstLastBufferAggregator(
      BaseLongColumnValueSelector timeSelector,
      BufferAggregator delegate
  )
  {
    this.timeSelector = timeSelector;
    this.delegate = delegate;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, getInitialTimestamp());
    delegate.init(buf, position + VALUE_OFFSET);
  }

  abstract Long getInitialTimestamp();

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new SerializablePair<>(
        buf.getLong(position),
        delegate.get(buf, position + VALUE_OFFSET)
    );
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return delegate.getLong(buf, position + VALUE_OFFSET);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return delegate.getFloat(buf, position + VALUE_OFFSET);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return delegate.getDouble(buf, position + VALUE_OFFSET);
  }

  @Override
  public void relocate(
      final int oldPosition,
      final int newPosition,
      final ByteBuffer oldBuffer,
      final ByteBuffer newBuffer
  )
  {
    newBuffer.putLong(newPosition, oldBuffer.getLong(oldPosition));
    delegate.relocate(
        oldPosition + VALUE_OFFSET,
        newPosition + VALUE_OFFSET,
        oldBuffer,
        newBuffer
    );
  }

  @Override
  public boolean isNull(final ByteBuffer buf, final int position)
  {
    return delegate.isNull(buf, position);
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("delegate", delegate);
  }
}
