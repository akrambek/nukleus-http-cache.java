/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeaderOrDefault;

import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class CacheableRequest extends AnswerableByCacheRequest
{
    int responseSlot = Slab.NO_SLOT;
    int responseHeadersSize;
    int responseSize;
    final MessageConsumer connect;
    final long connectRef;
    final LongSupplier supplyCorrelationId;
    final LongSupplier supplyStreamId;
    protected CacheState state;

    public enum CacheState
    {
        COMMITING, COMMITTED, PURGED
    }

    public CacheableRequest(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        MessageConsumer connect,
        long connectRef,
        LongSupplier supplyCorrelationId,
        LongSupplier supplyStreamId,
        int requestURLHash,
        int requestSlot,
        int requestSize,
        RouteManager router,
        short authScope,
        String etag)
    {
        super(acceptName,
              acceptReply,
              acceptReplyStreamId,
              acceptCorrelationId,
              router,
              requestSlot,
              requestSize,
              requestURLHash,
              authScope,
              etag);
        this.state = CacheState.COMMITING;
        this.supplyCorrelationId = supplyCorrelationId;
        this.supplyStreamId = supplyStreamId;

        this.connect = connect;
        this.connectRef = connectRef;
    }

    // TODO remove need for duplication
    public void copyRequestTo(
            MutableDirectBuffer buffer,
            BufferPool readFromBufferPool)
    {
        MutableDirectBuffer requestBuffer = readFromBufferPool.buffer(requestSlot());
        requestBuffer.getBytes(0, buffer, 0, requestSize());
    }

    public void cache(
            ListFW<HttpHeaderFW> responseHeaders,
            Cache cache,
            BufferPool cacheBufferPool)
    {
        etag(getHeaderOrDefault(responseHeaders, ETAG, etag()));

        setupResponseBuffer(cacheBufferPool);
        MutableDirectBuffer buffer = cacheBufferPool.buffer(responseSlot);
        final int headersSize = responseHeaders.sizeof();
        buffer.putBytes(responseSize, responseHeaders.buffer(), responseHeaders.offset(), headersSize);
        responseSize += headersSize;
        this.responseHeadersSize = headersSize;

        cache.notifyUncommitted(this);
    }

    private void setupResponseBuffer(BufferPool bufferPool)
    {
        this.responseSlot = bufferPool.acquire(acceptReplyStreamId());
        this.responseHeadersSize = 0;
        this.responseSize = 0;
    }

    public void cache(
            DataFW data,
            BufferPool cacheBufferPool)
    {
        if (state == CacheState.COMMITING)
        {
            OctetsFW payload = data.payload();
            int sizeof = payload.sizeof();
            if (responseSize + sizeof > cacheBufferPool.slotCapacity())
            {
                this.purge(cacheBufferPool);
            }
            else
            {
                MutableDirectBuffer buffer = cacheBufferPool.buffer(responseSlot);
                buffer.putBytes(responseSize, payload.buffer(), payload.offset(), sizeof);
                responseSize += sizeof;
            }
        }
    }

    public void cache(EndFW end, Cache cache)
    {
        if (state == CacheState.COMMITING)
        {
            state = CacheState.COMMITTED;
            cache.put(requestURLHash(), this);
        }
    }

    public void purge(BufferPool cacheBufferPool)
    {
        if (state != CacheState.PURGED)
        {
            super.purge(cacheBufferPool);
            if (responseSlot != Slab.NO_SLOT)
            {
                cacheBufferPool.release(responseSlot);
                responseSlot = Slab.NO_SLOT;
            }

            this.state = CacheState.PURGED;
        }
    }

    public long connectRef()
    {
        return connectRef;
    }

    public LongSupplier supplyCorrelationId()
    {
        return supplyCorrelationId;
    }

    public LongSupplier supplyStreamId()
    {
        return supplyStreamId;
    }

    // TODO hide abstraction
    public int responseSlot()
    {
        return responseSlot;
    }

    // TODO hide abstraction
    public int responseHeadersSize()
    {
        return responseHeadersSize;
    }

    // TODO hide abstraction
    public int responseSize()
    {
        return responseSize;
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(
        ListFW<HttpHeaderFW> responseHeadersRO,
        BufferPool cacheBufferPool)
    {
        MutableDirectBuffer responseBuffer = cacheBufferPool.buffer(responseSlot);
        return responseHeadersRO.wrap(responseBuffer, 0, responseHeadersSize);
    }

    public MessageConsumer connect()
    {
        return connect;
    }

    public MutableDirectBuffer getData(BufferPool bp)
    {
        return bp.buffer(responseSlot);
    }

}
