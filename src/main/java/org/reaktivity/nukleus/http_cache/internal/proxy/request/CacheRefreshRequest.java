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

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public class CacheRefreshRequest extends CacheableRequest
{

    private CacheEntry updatingEntry;
    private Cache cache;

    public CacheRefreshRequest(
            CacheableRequest req,
            int requestSlot,
            String etag,
            CacheEntry cacheEntry,
            Cache cache)
    {
        // TODO eliminate reference /GC duplication (Flyweight pattern?)
        super(req.acceptName,
              req.acceptReply,
              req.acceptReplyStreamId,
              req.acceptCorrelationId,
              req.connect,
              req.connectRef,
              req.supplyCorrelationId,
              req.supplyStreamId,
              req.requestURLHash(),
              requestSlot,
              req.requestSize(),
              req.router,
              req.authScope(),
              etag);
        this.updatingEntry = cacheEntry;
        this.cache = cache;
    }

    public void cache(
        ListFW<HttpHeaderFW> responseHeaders,
        Cache cache,
        BufferPool bufferPool)
    {
        if (responseHeaders.anyMatch(h ->
                ":status".equals(h.name().asString()) &&
                "200".equals(h.value().asString())))
        {
            super.cache(responseHeaders, cache, bufferPool);
        }
        else
        {
            this.purge(bufferPool);
        }
}

    @Override
    public Type getType()
    {
        return Type.CACHE_REFRESH;
    }

    @Override
    public void purge(BufferPool cacheBufferPool)
    {
        if (this.state != CacheState.COMMITTED)
        {
            this.cache.purge(updatingEntry);
        }
        super.purge(cacheBufferPool);
    }

}
