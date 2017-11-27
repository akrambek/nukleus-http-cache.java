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

package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request.Type;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public class Cache
{

    final Writer writer;
    final Int2ObjectHashMap<CacheEntry> cachedEntries;
    final BufferPool cachedRequestBufferPool;
    final BufferPool cachedResponseBufferPool;
    final BufferPool responseBufferPool;
    final BufferPool requestBufferPool;
    final BufferPool subscriberBufferPool;
    final ListFW<HttpHeaderFW> cachedRequestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();
    final WindowFW windowRO = new WindowFW();

    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();
    final CacheControl requestCacheControlFW = new CacheControl();
    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Request> correlations;
    final Supplier<String> etagSupplier;
    final Long2ObjectHashMap<PendingCacheEntries> uncommittedRequests = new Long2ObjectHashMap<>();

    public Cache(
            LongObjectBiConsumer<Runnable> scheduler,
            MutableDirectBuffer writeBuffer,
            BufferPool bufferPool,
            Long2ObjectHashMap<Request> correlations,
            Supplier<String> etagSupplier)
    {
        this.scheduler = scheduler;
        this.correlations = correlations;
        this.writer = new Writer(writeBuffer, bufferPool.duplicate());
        this.cachedRequestBufferPool = bufferPool;
        this.requestBufferPool = bufferPool;
        this.cachedResponseBufferPool = bufferPool.duplicate();
        this.responseBufferPool = bufferPool.duplicate();
        this.subscriberBufferPool = bufferPool.duplicate();
        this.cachedEntries = new Int2ObjectHashMap<>();
        this.etagSupplier = etagSupplier;
    }

    public void put(
        int requestUrlHash,
        CacheableRequest request)
    {
        CacheEntry oldCacheEntry = cachedEntries.get(requestUrlHash);
        boolean expectSubscribers = request.getType() == Type.INITIAL_REQUEST ? true: oldCacheEntry.expectSubscribers();
        CacheEntry cacheEntry = new CacheEntry(
                this,
                request,
                expectSubscribers);

        if (cacheEntry.isIntendedForSingleUser())
        {
            cacheEntry.purge();
        }
        else if (oldCacheEntry == null)
        {
            updateCache(requestUrlHash, cacheEntry);
        }
        else if (oldCacheEntry.isUpdatedBy(request))
        {
            updateCache(requestUrlHash, cacheEntry);

            oldCacheEntry.subscribers(subscriber ->
            {
                if (!this.serveRequest(
                        cacheEntry,
                        subscriber.getRequestHeaders(requestHeadersRO, subscriberBufferPool),
                        subscriber.authScope(),
                        subscriber))
                {
                    final MessageConsumer acceptReply = subscriber.acceptReply();
                    final long acceptReplyStreamId = subscriber.acceptReplyStreamId();
                    final long acceptCorrelationId = subscriber.acceptCorrelationId();
                    this.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
                }
            });
            oldCacheEntry.purge();
        }
        else
        {
            cacheEntry.purge();
            if (request.getType() == Request.Type.CACHE_REFRESH)
            {
                oldCacheEntry.refresh(request);
            }
        }
    }

    private void updateCache(
            int requestUrlHash,
            CacheEntry cacheEntry)
    {
        cacheEntry.commit();
        cachedEntries.put(requestUrlHash, cacheEntry);
        PendingCacheEntries result = this.uncommittedRequests.remove(requestUrlHash);
        if (result != null)
        {
            result.addSubscribers(cacheEntry);
        }
    }

    public boolean handleInitialRequest(
            int requestURLHash,
            ListFW<HttpHeaderFW> request,
            short authScope,
            CacheableRequest cacheableRequest)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        if (cacheEntry != null)
        {
            return serveRequest(cacheEntry, request, authScope, cacheableRequest);
        }
        else
        {
            return false;
        }
    }

    public void handleOnUpdateRequest(
            int requestURLHash,
            OnUpdateRequest onUpdateRequest,
            ListFW<HttpHeaderFW> requestHeaders,
            short authScope)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        PendingCacheEntries uncommittedRequest = this.uncommittedRequests.get(requestURLHash);
        String ifNoneMatch = HttpHeadersUtil.getHeader(requestHeaders, HttpHeaders.IF_NONE_MATCH);
        assert ifNoneMatch != null;
        if (uncommittedRequest != null && ifNoneMatch.contains(uncommittedRequest.etag()))
        {
            uncommittedRequest.subscribe(onUpdateRequest);
        }
        else if(cacheEntry == null)
        {
            final MessageConsumer acceptReply = onUpdateRequest.acceptReply();
            final long acceptReplyStreamId = onUpdateRequest.acceptReplyStreamId();
            final long acceptCorrelationId = onUpdateRequest.acceptCorrelationId();
            writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
        }
        else if (cacheEntry.isUpdateRequestForThisEntry(requestHeaders))
        {
            cacheEntry.subscribeToUpdate(onUpdateRequest);
        }
        else if (cacheEntry.canServeRequest(requestHeaders, authScope))
        {
            cacheEntry.serveClient(onUpdateRequest);
        }
        else
        {
            final MessageConsumer acceptReply = onUpdateRequest.acceptReply();
            final long acceptReplyStreamId = onUpdateRequest.acceptReplyStreamId();
            final long acceptCorrelationId = onUpdateRequest.acceptCorrelationId();
            writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
        }
    }

    private boolean serveRequest(
            CacheEntry entry,
            ListFW<HttpHeaderFW> request,
            short authScope,
            AnswerableByCacheRequest cacheableRequest)
    {
        if (entry.canServeRequest(request, authScope))
        {
            entry.serveClient(cacheableRequest);
            return true;
        }
        return false;
    }

    public void notifyUncommitted(CacheableRequest request)
    {
        if (request.getType() == Request.Type.INITIAL_REQUEST)
        {
            this.uncommittedRequests.computeIfAbsent(request.requestURLHash(), p -> new PendingCacheEntries(request));
        }
    }

    public void purge(CacheEntry entry)
    {
        this.cachedEntries.remove(entry.requestUrl());
        entry.purge();
    }

}
