/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache.emulated;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.Request;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.InitialRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.PreferWaitIfNoneMatchRequest;
import org.reaktivity.nukleus.http_cache.internal.stream.BudgetManager;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

public class Cache
{
    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final ListFW<HttpHeaderFW> cachedRequestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedRequest1HeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponse1HeadersRO = new HttpBeginExFW().headers();

    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();

    final WindowFW windowRO = new WindowFW();

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();

    public final BufferPool cachedRequestBufferPool;
    public final BufferPool cachedResponseBufferPool;

    final Writer writer;
    final BudgetManager budgetManager;
    final Int2CacheHashMapWithLRUEviction cachedEntries;
    final BufferPool cachedRequest1BufferPool;
    final BufferPool cachedResponse1BufferPool;

    final BufferPool refreshBufferPool;
    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;

    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Request> correlations;
    final LongSupplier supplyTrace;
    final Int2ObjectHashMap<PendingCacheEntries> uncommittedRequests;
    final Int2ObjectHashMap<PendingInitialRequests> pendingInitialRequestsMap;
    final HttpCacheCounters counters;

    public Cache(
        RouteManager router,
        LongObjectBiConsumer<Runnable> scheduler,
        BudgetManager budgetManager,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        BufferPool cacheBufferPool,
        Long2ObjectHashMap<Request> correlations,
        HttpCacheCounters counters,
        LongConsumer entryCount,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId)
    {
        this.scheduler = scheduler;
        this.budgetManager = budgetManager;
        this.correlations = correlations;
        this.writer = new Writer(router, supplyTypeId, writeBuffer);
        this.refreshBufferPool = new CountingBufferPool(
                requestBufferPool.duplicate(),
                counters.supplyCounter.apply("http-cache.refresh.request.acquires"),
                counters.supplyCounter.apply("http-cache.refresh.request.releases"));
        this.cachedRequestBufferPool = new CountingBufferPool(
                cacheBufferPool,
                counters.supplyCounter.apply("http-cache.cached.request.acquires"),
                counters.supplyCounter.apply("http-cache.cached.request.releases"));
        this.cachedResponseBufferPool = new CountingBufferPool(
                cacheBufferPool.duplicate(),
                counters.supplyCounter.apply("http-cache.cached.response.acquires"),
                counters.supplyCounter.apply("http-cache.cached.response.releases"));
        this.cachedRequest1BufferPool = cacheBufferPool.duplicate();
        this.cachedResponse1BufferPool = cacheBufferPool.duplicate();
        this.requestBufferPool = requestBufferPool.duplicate();
        this.responseBufferPool = requestBufferPool.duplicate();
        this.cachedEntries = new Int2CacheHashMapWithLRUEviction(entryCount);
        this.counters = counters;
        this.supplyTrace = requireNonNull(supplyTrace);
        this.uncommittedRequests = new Int2ObjectHashMap<>();
        this.pendingInitialRequestsMap = new Int2ObjectHashMap<>();
    }

    public void put(
        int requestHash,
        CacheableRequest request)
    {
        CacheEntry oldCacheEntry = cachedEntries.get(requestHash);
        if (oldCacheEntry == null)
        {
            CacheEntry cacheEntry = new CacheEntry(
                    this,
                    request,
                    true,
                    supplyTrace);
            updateCache(requestHash, cacheEntry);
            cacheEntry.sendHttpPushPromise(request);
        }
        else
        {
            boolean expectSubscribers = (request.getType() == Request.Type.INITIAL_REQUEST) || oldCacheEntry.expectSubscribers();
            CacheEntry cacheEntry = new CacheEntry(
                    this,
                    request,
                    expectSubscribers,
                    supplyTrace);

            if (cacheEntry.isIntendedForSingleUser())
            {
                cacheEntry.purge();
            }
            else if (oldCacheEntry.isUpdatedBy(request))
            {
                updateCache(requestHash, cacheEntry);

                boolean notVaries = oldCacheEntry.doesNotVaryBy(cacheEntry);
                if (notVaries)
                {
                    oldCacheEntry.subscribers(cacheEntry::serveClient);
                }
                else
                {
                    oldCacheEntry.subscribers(subscriber ->
                    {
                        final MessageConsumer acceptReply = subscriber.acceptReply();
                        final long acceptRouteId = subscriber.acceptRouteId();
                        final long acceptReplyId = subscriber.acceptReplyId();
                        this.writer.do503AndAbort(acceptReply,
                                                acceptRouteId,
                                                acceptReplyId,
                                                supplyTrace.getAsLong());

                        // count all responses
                        counters.responses.getAsLong();

                        // count ABORTed responses
                        counters.responsesAbortedVary.getAsLong();
                    });
                }
                oldCacheEntry.purge();
            }
            else
            {
                if (oldCacheEntry.isSelectedForUpdate(request))
                {
                    oldCacheEntry.cachedRequest.updateResponseHeader(request.getResponseHeaders(responseHeadersRO,
                            cachedResponse1BufferPool));
                }
                cacheEntry.purge();
            }
        }
    }

    private void updateCache(
            int requestHash,
            CacheEntry cacheEntry)
    {
        cacheEntry.commit();
        cachedEntries.put(requestHash, cacheEntry);
        PendingCacheEntries result = this.uncommittedRequests.remove(requestHash);
        if (result != null)
        {
            result.addSubscribers(cacheEntry);
        }
    }

    public boolean handleInitialRequest(
        int requestHash,
        ListFW<HttpHeaderFW> request,
        short authScope,
        CacheableRequest cacheableRequest)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestHash);
        if (cacheEntry != null)
        {
            return serveRequest(cacheEntry, request, authScope, cacheableRequest);
        }
        else
        {
            return false;
        }
    }

    public void servePendingInitialRequests(
        int requestHash)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestHash);
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestHash);
        if (pendingInitialRequests != null)
        {
            pendingInitialRequests.removeSubscribers(s ->
            {
                boolean served = false;

                if (cacheEntry != null)
                {
                    served = serveRequest(cacheEntry, s.getRequestHeaders(requestHeadersRO),
                            s.authScope(), s);
                }

                if (!served)
                {
                    sendPendingInitialRequest(s);
                }
            });
        }
    }

    public void sendPendingInitialRequests(
        int requestHash)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestHash);
        if (pendingInitialRequests != null)
        {
            final PendingInitialRequests newPendingInitialRequests = pendingInitialRequests.withNextInitialRequest();
            if (newPendingInitialRequests != null)
            {
                pendingInitialRequestsMap.put(requestHash, newPendingInitialRequests);
                sendPendingInitialRequest(newPendingInitialRequests.initialRequest());
            }
        }
    }

    private void sendPendingInitialRequest(
        final InitialRequest request)
    {
        long connectRouteId = request.connectRouteId();
        long connectInitialId = request.supplyInitialId().applyAsLong(connectRouteId);
        MessageConsumer connectInitial = request.supplyReceiver().apply(connectInitialId);
        long connectReplyId = request.supplyReplyId().applyAsLong(connectInitialId);
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(requestHeadersRO);

        correlations.put(connectReplyId, request);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent pending request]\n",
                    currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        writer.doHttpRequest(connectInitial, connectRouteId, connectInitialId, supplyTrace.getAsLong(),
                builder -> requestHeaders.forEach(h ->  builder.item(item -> item.name(h.name()).value(h.value()))));
        writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, supplyTrace.getAsLong());
    }

    public boolean hasPendingInitialRequests(
        int requestHash)
    {
        return pendingInitialRequestsMap.containsKey(requestHash);
    }

    public void addPendingRequest(
        InitialRequest initialRequest)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.get(initialRequest.requestHash());
        pendingInitialRequests.subscribe(initialRequest);
    }

    public void createPendingInitialRequests(
        InitialRequest initialRequest)
    {
        pendingInitialRequestsMap.put(initialRequest.requestHash(), new PendingInitialRequests(initialRequest));
    }

    public void handlePreferWaitIfNoneMatchRequest(
        int requestHash,
        PreferWaitIfNoneMatchRequest preferWaitRequest,
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestHash);
        PendingCacheEntries uncommittedRequest = this.uncommittedRequests.get(requestHash);

        String ifNoneMatch = HttpHeadersUtil.getHeader(requestHeaders, HttpHeaders.IF_NONE_MATCH);
        assert ifNoneMatch != null;
        if (uncommittedRequest != null && ifNoneMatch.contains(uncommittedRequest.etag())
                && doesNotVary(requestHeaders, uncommittedRequest.request))
        {
            uncommittedRequest.subscribe(preferWaitRequest);
        }
        else if (cacheEntry == null)
        {
            final MessageConsumer acceptReply = preferWaitRequest.acceptReply();
            final long acceptRouteId = preferWaitRequest.acceptRouteId();
            final long acceptReplyId = preferWaitRequest.acceptReplyId();
            writer.do503AndAbort(acceptReply, acceptRouteId, acceptReplyId, supplyTrace.getAsLong());

            // count all responses
            counters.responses.getAsLong();

            // count ABORTed responses
            counters.responsesAbortedEvicted.getAsLong();
        }
        else if (cacheEntry.isUpdateRequestForThisEntry(requestHeaders))
        {
            // TODO return value ??
            cacheEntry.subscribeWhenNoneMatch(preferWaitRequest);
        }
        else if (cacheEntry.canServeUpdateRequest(requestHeaders))
        {
            cacheEntry.serveClient(preferWaitRequest);
        }
        else
        {
            final MessageConsumer acceptReply = preferWaitRequest.acceptReply();
            final long acceptRouteId = preferWaitRequest.acceptRouteId();
            final long acceptReplyId = preferWaitRequest.acceptReplyId();

            writer.do503AndAbort(acceptReply, acceptRouteId, acceptReplyId, supplyTrace.getAsLong());

            // count all responses
            counters.responses.getAsLong();

            // count ABORTed responses
            counters.responsesAbortedMiss.getAsLong();
        }
    }

    private boolean doesNotVary(
        ListFW<HttpHeaderFW> requestHeaders,
        InitialRequest request)
    {
        ListFW<HttpHeaderFW> cachedRequestHeaders = request.getRequestHeaders(cachedRequestHeadersRO);
        ListFW<HttpHeaderFW> cachedResponseHeaders = request.getResponseHeaders(cachedResponseHeadersRO);
        return CacheUtils.doesNotVary(requestHeaders, cachedResponseHeaders, cachedRequestHeaders);
    }

    private boolean serveRequest(
        CacheEntry entry,
        ListFW<HttpHeaderFW> request,
        short authScope,
        AnswerableByCacheRequest cacheableRequest)
    {
        if (entry.canServeRequest(request, authScope))
        {
            final String requestAuthorizationHeader = getHeader(request, AUTHORIZATION);
            entry.recentAuthorizationHeader(requestAuthorizationHeader);

            boolean etagMatched = CacheUtils.isMatchByEtag(request, entry.cachedRequest.etag());
            if (etagMatched)
            {
                send304(entry, cacheableRequest);
            }
            else
            {
                entry.serveClient(cacheableRequest);
            }

            return true;
        }
        return false;
    }

    private void send304(
        CacheEntry entry,
        AnswerableByCacheRequest request)
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n",
                    currentTimeMillis(), request.acceptReplyId(), "304");
        }

        writer.doHttpResponse(request.acceptReply(), request.acceptRouteId(),
                request.acceptReplyId(), supplyTrace.getAsLong(), e -> e.item(h -> h.name(STATUS).value("304"))
                      .item(h -> h.name(ETAG).value(entry.cachedRequest.etag())));
        writer.doHttpEnd(request.acceptReply(), request.acceptRouteId(), request.acceptReplyId(), supplyTrace.getAsLong());

        request.purge();

        // count all responses
        counters.responses.getAsLong();
    }

    public void notifyUncommitted(
        InitialRequest request)
    {
        this.uncommittedRequests.computeIfAbsent(request.requestHash(), p -> new PendingCacheEntries(request));
    }

    public void removeUncommitted(
        InitialRequest request)
    {
        this.uncommittedRequests.computeIfPresent(request.requestHash(), (k, v) ->
        {
            v.removeSubscribers(subscriber ->
            {
                final MessageConsumer acceptReply = subscriber.acceptReply();
                final long acceptRouteId = subscriber.acceptRouteId();
                final long acceptReplyId = subscriber.acceptReplyId();
                this.writer.do503AndAbort(acceptReply, acceptRouteId, acceptReplyId, supplyTrace.getAsLong());

                // count all responses
                counters.responses.getAsLong();

                // count ABORTed responses
                counters.responsesAbortedUncommited.getAsLong();
            });
            return null;
        });
    }

    public void purge(
        CacheEntry entry)
    {
        this.cachedEntries.remove(entry.requestUrl());
        entry.purge();
    }

    public boolean purgeOld()
    {
        return this.cachedEntries.purgeLRU();
    }

}