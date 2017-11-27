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

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class AnswerableByCacheRequest extends Request
{

    private int requestSlot;
    private final int requestSize;
    private final int requestURLHash;
    private final short authScope;
    private String etag;

    public AnswerableByCacheRequest(
            String acceptName,
            MessageConsumer acceptReply,
            long acceptReplyStreamId,
            long acceptCorrelationId,
            RouteManager router,
            int requestSlot,
            int requestSize,
            int requestURLHash,
            short authScope,
            String etag)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId, router);
        this.requestSlot = requestSlot;
        this.requestSize = requestSize;
        this.requestURLHash = requestURLHash;
        this.authScope = authScope;
        this.etag = etag;
    }

    public final ListFW<HttpHeaderFW> getRequestHeaders(
            ListFW<HttpHeaderFW> requestHeadersRO,
            BufferPool pool)
    {
        final MutableDirectBuffer buffer = pool.buffer(requestSlot);
        return requestHeadersRO.wrap(buffer, 0, requestSize);
    }

    public final short authScope()
    {
        return authScope;
    }

    public final String etag()
    {
        return etag;
    }

    public final int requestURLHash()
    {
        return requestURLHash;
    }

    public final int requestSize()
    {
        return requestSize;
    }

    public final int requestSlot()
    {
        return requestSlot;
    }

    public void purge(BufferPool bufferPool)
    {
        if (requestSlot != Slab.NO_SLOT)
        {
            bufferPool.release(requestSlot);
            this.requestSlot = Slab.NO_SLOT;
        }
    }

    protected void etag(String etag)
    {
        this.etag = etag;
    }

}