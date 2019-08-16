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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpCacheProxyCacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpCacheProxyFactory;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpCacheProxyRequest;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferenceApplied;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.CACHE_ENTRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

public class DefaultCache
{
    public static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();

    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();

    final BufferPool cachedRequestBufferPool;
    final BufferPool cachedResponseBufferPool;

    final Writer writer;
    final Int2CacheHashMapWithLRUEviction cachedEntries;

    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<HttpCacheProxyRequest> correlations;
    final LongSupplier supplyTrace;
    final Int2ObjectHashMap<PendingInitialRequests> pendingInitialRequestsMap;
    final HttpCacheCounters counters;
    final SignalingExecutor executor;

    public DefaultCache(
        LongObjectBiConsumer<Runnable> scheduler,
        MutableDirectBuffer writeBuffer,
        BufferPool cacheBufferPool,
        Long2ObjectHashMap<HttpCacheProxyRequest> correlations,
        HttpCacheCounters counters,
        LongConsumer entryCount,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor)
    {
        this.pendingInitialRequestsMap = new Int2ObjectHashMap<>();
        this.scheduler = scheduler;
        this.correlations = correlations;
        this.writer = new Writer(supplyTypeId, writeBuffer);
        this.cachedRequestBufferPool = new CountingBufferPool(
                cacheBufferPool,
                counters.supplyCounter.apply("http-cache.cached.request.acquires"),
                counters.supplyCounter.apply("http-cache.cached.request.releases"));
        this.cachedResponseBufferPool = new CountingBufferPool(
                cacheBufferPool.duplicate(),
                counters.supplyCounter.apply("http-cache.cached.response.acquires"),
                counters.supplyCounter.apply("http-cache.cached.response.releases"));
        this.cachedEntries = new Int2CacheHashMapWithLRUEviction(entryCount);
        this.counters = counters;
        this.supplyTrace = requireNonNull(supplyTrace);
        this.executor = executor;
    }

    public DefaultCacheEntry get(
        int requestHash)
    {
        return cachedEntries.get(requestHash);
    }

    public DefaultCacheEntry supply(
        int requestHash)
    {
        DefaultCacheEntry defaultCacheEntry = cachedEntries.get(requestHash);
        if (defaultCacheEntry == null)
        {
            defaultCacheEntry = new DefaultCacheEntry(
                    this,
                    requestHash,
                    cachedRequestBufferPool,
                    cachedResponseBufferPool);
            updateCache(requestHash, defaultCacheEntry);
        }

        return defaultCacheEntry;
    }

    public void signalForCacheEntry(
        HttpCacheProxyCacheableRequest defaultRequest,
        long signalId)
    {
            writer.doSignal(defaultRequest.getSignaler(),
                defaultRequest.acceptRouteId,
                defaultRequest.acceptStreamId,
                supplyTrace.getAsLong(),
                signalId);
    }

    public void signalForUpdatedCacheEntry(int requestHash)
    {
       this.sendSignalToPendingInitialRequestsSubscribers(requestHash, CACHE_ENTRY_UPDATED_SIGNAL);
    }

    public void signalAbortAllSubscribers(
        int requestHash)
    {
        this.sendSignalToPendingInitialRequestsSubscribers(requestHash, ABORT_SIGNAL);
    }

    private void sendSignalToPendingInitialRequestsSubscribers(
        int requestHash,
        long signal)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.get(requestHash);
        if (pendingInitialRequests != null)
        {
            pendingInitialRequests.subcribers().forEach(request ->
            {
                writer.doSignal(request.getSignaler(),
                                request.acceptRouteId,
                                request.acceptStreamId,
                                supplyTrace.getAsLong(),
                                signal);
            });
        }
    }

    public boolean handleCacheableRequest(
        HttpCacheProxyFactory streamFactory,
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope,
        HttpCacheProxyCacheableRequest request)
    {
        boolean canHandleRequest = false;
        final DefaultCacheEntry defaultCacheEntry = cachedEntries.get(request.requestHash());

        if (defaultCacheEntry != null && serveRequest(streamFactory,
                                                         defaultCacheEntry,
                                                         requestHeaders,
                                                         authScope,
                                                      request))
        {
            canHandleRequest = true;
        }
        else if (hasPendingInitialRequests(request.requestHash()))
        {
            addPendingRequest(request);
            canHandleRequest = true;
        }
        else if (requestHeaders.anyMatch(CacheDirectives.IS_ONLY_IF_CACHED))
        {
            send504(request);
            canHandleRequest = true;
        }

        return canHandleRequest;
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

    public void send304ToPendingInitialRequests(
        int requestHash)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestHash);
        if (pendingInitialRequests != null)
        {
            DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);
            pendingInitialRequests.subcribers().forEach(request ->
            {
                send304(cacheEntry, request);
                request.purge();
            });
            pendingInitialRequests.removeAllSubscribers();
        }
    }

    public void removeAllPendingInitialRequests(
        int requestHash)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestHash);
        if (pendingInitialRequests != null)
        {
            pendingInitialRequests.removeAllSubscribers();
        }
    }

    public void addPendingRequest(
        HttpCacheProxyCacheableRequest initialRequest)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.get(initialRequest.requestHash());
        pendingInitialRequests.subscribe(initialRequest);
    }

    public void createPendingInitialRequests(
        HttpCacheProxyCacheableRequest initialRequest)
    {
        pendingInitialRequestsMap.put(initialRequest.requestHash(), new PendingInitialRequests(initialRequest));
    }

    public void removePendingInitialRequest(
        HttpCacheProxyCacheableRequest request)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.get(request.requestHash());
        if (pendingInitialRequests != null)
        {
            pendingInitialRequests.removeSubscriber(request);
            if(pendingInitialRequests.numberOfSubscribers() == 0)
            {
                pendingInitialRequestsMap.remove(request.requestHash());
            }
        }
    }

    public void purge(
        int requestHash)
    {
        DefaultCacheEntry cacheEntry = this.cachedEntries.remove(requestHash);
        if (cacheEntry != null)
        {
            cacheEntry.purge();
        }
    }

    private void updateCache(
        int requestHash,
        DefaultCacheEntry cacheEntry)
    {
        cachedEntries.put(requestHash, cacheEntry);
    }

    private boolean serveRequest(
        HttpCacheProxyFactory streamFactory,
        DefaultCacheEntry entry,
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope,
        HttpCacheProxyCacheableRequest defaultRequest)
    {
        if (entry.canServeRequest(requestHeaders, authScope))
        {
            final String requestAuthorizationHeader = getHeader(requestHeaders, AUTHORIZATION);
            entry.recentAuthorizationHeader(requestAuthorizationHeader);

            boolean etagMatched = CacheUtils.isMatchByEtag(requestHeaders, entry.etag());
            if (etagMatched)
            {
                send304(entry, defaultRequest);
            }
            else
            {
                streamFactory.correlations.put(defaultRequest.connectReplyId, defaultRequest);
                signalForCacheEntry(defaultRequest, CACHE_ENTRY_SIGNAL);
                this.counters.responsesCached.getAsLong();
            }

            return true;
        }
        return false;
    }

    public void send304(
        DefaultCacheEntry entry,
        HttpCacheProxyCacheableRequest request)
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n",
                    currentTimeMillis(), request.acceptReplyId(), "304");
        }

        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(requestHeadersRO);

        if (isPreferIfNoneMatch(requestHeaders))
        {
            String preferWait = getHeader(requestHeaders, PREFER);
            writer.doHttpResponse(request.acceptReply,
                                  request.acceptRouteId,
                                  request.acceptReplyId(),
                                  supplyTrace.getAsLong(),
                                  e -> e.item(h -> h.name(STATUS).value("304"))
                                        .item(h -> h.name(ETAG).value(entry.etag()))
                                        .item(h -> h.name(PREFERENCE_APPLIED).value(preferWait)));
        }
        else
        {
            writer.doHttpResponse(request.acceptReply,
                                  request.acceptRouteId,
                                  request.acceptReplyId(),
                                  supplyTrace.getAsLong(),
                                  e -> e.item(h -> h.name(STATUS).value("304"))
                                        .item(h -> h.name(ETAG).value(entry.etag())));
        }

        writer.doHttpEnd(request.acceptReply, request.acceptRouteId, request.acceptReplyId(), supplyTrace.getAsLong());
        request.purge();

        // count all responses
        counters.responses.getAsLong();
    }

    private void sendPendingInitialRequest(
        final HttpCacheProxyCacheableRequest request)
    {
        long connectRouteId = request.connectRouteId;
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


    public void purgeEntriesForNonPendingRequests()
    {
         cachedEntries.getCachedEntries().forEach((i, cacheEntry)  ->
         {
             if (!hasPendingInitialRequests(cacheEntry.requestHash()))
             {
                 this.purge(cacheEntry.requestHash());
             }
         });
    }

    public boolean isUpdatedByResponseHeadersToRetry(
        HttpCacheProxyCacheableRequest request,
        ListFW<HttpHeaderFW> responseHeaders)
    {
        ListFW<HttpHeaderFW> requestHeaders = null;
        requestHeaders = request.getRequestHeaders(requestHeadersRO);

        if (isPreferWait(requestHeaders)
            && !isPreferenceApplied(responseHeaders))
        {
            String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
            String etag = request.etag();
            String newEtag = getHeader(responseHeaders, ETAG);
            boolean etagMatches = false;
            assert status != null;

            if (etag != null && newEtag != null)
            {
                etagMatches = status.equals(HttpStatus.OK_200) && etag.equals(newEtag);

                if (etagMatches)
                {
                    DefaultCacheEntry cacheEntry = cachedEntries.get(request.requestHash());
                    cacheEntry.updateResponseHeader(responseHeaders);
                }
            }

            boolean notModified = status.equals(HttpStatus.NOT_MODIFIED_304) || etagMatches;

            return !notModified;
        }

        return true;
    }

    public boolean isUpdatedByEtagToRetry(
        HttpCacheProxyCacheableRequest request,
        DefaultCacheEntry cacheEntry)
    {
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(requestHeadersRO);
        ListFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();
        if (isPreferWait(requestHeaders)
            && !isPreferenceApplied(responseHeaders))
        {
            String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
            String etag = request.etag();
            String newEtag = cacheEntry.etag();
            assert status != null;

            if (etag != null && newEtag != null)
            {
                return !(status.equals(HttpStatus.OK_200) && etag.equals(newEtag));
            }
        }

        return true;
    }

    private void send504(
        HttpCacheProxyCacheableRequest request)
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), request.acceptReplyId(), "504");
        }

        writer.doHttpResponse(request.acceptReply,
                              request.acceptRouteId,
                              request.acceptReplyId(),
                              supplyTrace.getAsLong(), e ->
                                                    e.item(h -> h.name(STATUS)
                                                                 .value("504")));
        writer.doAbort(request.acceptReply,
                       request.acceptRouteId,
                       request.acceptReplyId(),
                       supplyTrace.getAsLong());
        request.purge();

        // count all responses
        counters.responses.getAsLong();
    }
}
