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

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_AGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_STALE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MIN_FRESH;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.S_MAXAGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheEntryState.CAN_REFRESH;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheEntryState.REFRESHING;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.sameAuthorizationScope;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl.getSurrogateAge;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl.getSurrogateFreshnessExtension;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.WARNING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheRefreshRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public final class CacheEntry
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    private final CacheControl cacheControlFW = new CacheControl();

    private final Cache cache;
    private int clientCount = 0;

    private Instant lazyInitiatedResponseReceivedAt;
    private Instant lazyInitiatedResponseStaleAt;

    private final CacheableRequest cachedRequest;

    private List<OnUpdateRequest> subscribers = new ArrayList<OnUpdateRequest>(); // TODO, lazy init

    boolean expectSubscribers;

    private CacheRefreshRequest pollingRequest;

    private CacheEntryState state;

    private long pollAt = -1;

    public CacheEntry(
            Cache cache,
            CacheableRequest request,
            boolean expectSubscribers)
    {
        this.cache = cache;
        this.cachedRequest = request;
        this.expectSubscribers = expectSubscribers;
        this.state = CacheEntryState.INITIALIZED;
    }

    public void commit()
    {
        final int freshnessExtension = getSurrogateFreshnessExtension(getCachedResponseHeaders());
        if (freshnessExtension > 0)
        {
            this.state = CacheEntryState.REFRESHING;
            pollBackend();
        }
        else
        {
            this.state = CacheEntryState.CANT_REFRESH;
        }
    }

    private void pollBackend()
    {
        if (expectSubscribers || !subscribers.isEmpty())
        {
            this.state = CacheEntryState.REFRESHING;
            int surrogateMaxAge = getSurrogateAge(getCachedResponseHeaders());
            if (this.pollAt == -1)
            {
                this.pollAt = Instant.now().plusSeconds(surrogateMaxAge).toEpochMilli();
            }
            else
            {
                this.pollAt += surrogateMaxAge * 1000;
            }
            cache.scheduler.accept(pollAt, this::sendRefreshRequest);
            expectSubscribers = false;
        }
        else
        {
            this.state = CacheEntryState.CAN_REFRESH;
        }
    }

    private void sendRefreshRequest()
    {
        if (this.state != CacheEntryState.PURGED)
        {
            MessageConsumer connect = cachedRequest.connect();
            long connectStreamId = cachedRequest.supplyStreamId().getAsLong();
            long connectRef = cachedRequest.connectRef();
            long connectCorrelationId = cachedRequest.supplyCorrelationId().getAsLong();
            ListFW<HttpHeaderFW> requestHeaders = getCachedRequest();
            final String etag = this.cachedRequest.etag();
            cache.writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId,
                    builder ->
                        {
                            requestHeaders.forEach(
                                    h ->  builder.item(item -> item.name(h.name()).value(h.value())));
                            builder.item(item -> item.name(HttpHeaders.IF_NONE_MATCH).value(etag));
                        });
            cache.writer.doHttpEnd(connect, connectStreamId);

            // duplicate request into new slot (TODO optimize to single request)
            int newSlot = cache.requestBufferPool.acquire(connectStreamId);
            if (newSlot == NO_SLOT)
            {
                throw new RuntimeException("Cache out of space, please reconfigure");  // TODO reconsider hard fail??
            }
            MutableDirectBuffer newBuffer = cache.requestBufferPool.buffer(newSlot);
            this.cachedRequest.copyRequestTo(newBuffer, cache.cachedResponseBufferPool);

            final CacheRefreshRequest refreshRequest = new CacheRefreshRequest(
                    cachedRequest,
                    newSlot,
                    cache.etagSupplier.get(),
                    this,
                    this.cache);
            this.pollingRequest = refreshRequest;
            cache.correlations.put(connectCorrelationId, refreshRequest);
        }
    }

    private void handleEndOfStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        this.removeClient();
    }

    public void serveClient(
            AnswerableByCacheRequest streamCorrelation)
    {
        switch (this.state)
        {
            case PURGED:
                throw new IllegalStateException("Can not serve client when entry is purged");
            default:
                sendResponseToClient(streamCorrelation, true);
                break;
        }
        streamCorrelation.purge(cache.requestBufferPool);
    }

    private void sendResponseToClient(
            AnswerableByCacheRequest request,
            boolean injectWarnings)
    {
        addClient();
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();

        ServeFromCacheStream serveFromCacheStream = new ServeFromCacheStream(
                request,
                cachedRequest.responseSlot(),   // TODO hide abstraction
                cachedRequest.responseHeadersSize(),
                cachedRequest.responseSize(),
                this::handleEndOfStream);
        request.setThrottle(serveFromCacheStream);

        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = x -> responseHeaders
                .forEach(h -> x.item(y -> y.representation((byte) 0).name(h.name()).value(h.value())));

        final MessageConsumer acceptReply = request.acceptReply();
        long acceptReplyStreamId = request.acceptReplyStreamId();
        long acceptReplyRef = request.acceptRef();
        long acceptCorrelationId = request.acceptCorrelationId();

        // TODO should reduce freshness extension by how long it has aged
        int freshnessExtension = SurrogateControl.getSurrogateFreshnessExtension(responseHeaders);
        if (freshnessExtension > 0 && this.state == REFRESHING || this.state == CAN_REFRESH)
        {
            expectSubscribers = true;
            this.cache.writer.doHttpResponseWithUpdatedCacheControl(
                    acceptReply,
                    acceptReplyStreamId,
                    acceptReplyRef,
                    acceptCorrelationId,
                    cacheControlFW,
                    responseHeaders,
                    freshnessExtension,
                    cachedRequest.etag());

            this.cache.writer.doHttpPushPromise(
                    request,
                    responseHeaders,
                    freshnessExtension,
                    cachedRequest.etag());
        }
        else
        {
            // TODO inject stale on above if (freshnessExtension > 0)?
            if (injectWarnings && isStale())
            {
                headers = headers.andThen(
                        x ->  x.item(h -> h.representation((byte) 0).name(WARNING).value(Cache.RESPONSE_IS_STALE))
                );
            }
            this.cache.writer.doHttpBegin(acceptReply, acceptReplyStreamId, acceptReplyRef, acceptCorrelationId, headers);
        }

        if(this.state == CacheEntryState.CAN_REFRESH)
        {
            pollBackend();
        }
    }

    public void purge()
    {
        switch (this.state)
        {
            case PURGED:
                break;
            default:
                this.state = CacheEntryState.PURGED;
                if (clientCount == 0)
                {
                    cachedRequest.purge(cache.cachedRequestBufferPool);
                }
                subscribers.stream().forEach(s ->
                {
                    MessageConsumer acceptReply = s.acceptReply();
                    long acceptReplyStreamId = s.acceptReplyStreamId();
                    long acceptCorrelationId = s.acceptCorrelationId();
                    cache.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
                    s.purge(cache.subscriberBufferPool);
                });
                subscribers.clear();
                break;
        }
    }

    class ServeFromCacheStream implements MessageConsumer
    {
        private final Request request;
        private int payloadWritten;
        private int responseSlot;
        private int responseHeaderSize;
        private int responseSize;
        private MessageConsumer onEnd;
        private int budget;

         ServeFromCacheStream(
            Request request,
            int responseSlot,
            int responseHeaderSize,
            int responseSize,
            MessageConsumer onEnd)
        {
            this.payloadWritten = 0;
            this.request = request;
            this.responseSlot = responseSlot;
            this.responseHeaderSize = responseHeaderSize;
            this.responseSize = responseSize - responseHeaderSize;
            this.onEnd = onEnd;
        }

        public void accept(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch(msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = CacheEntry.this.cache.windowRO.wrap(buffer, index, index + length);
                    writePayload(window.credit(), window.padding());
                    break;
                case ResetFW.TYPE_ID:
                default:
                    this.onEnd.accept(msgTypeId, buffer, index, length);
                    break;
            }
        }

        private void writePayload(int credit, int padding)
        {
            budget += credit;
            if (budget > padding)
            {
                final int toWrite = Math.min(budget - padding, responseSize - payloadWritten);
                final int offset = responseHeaderSize + payloadWritten;
                MutableDirectBuffer buffer = CacheEntry.this.cache.cachedResponseBufferPool.buffer(responseSlot);
                final MessageConsumer acceptReply = request.acceptReply();
                final long acceptReplyStreamId = request.acceptReplyStreamId();
                budget -= padding;
                CacheEntry.this.cache.writer.doHttpData(acceptReply, acceptReplyStreamId, buffer, offset, toWrite);
                payloadWritten += toWrite;
                if (payloadWritten == responseSize)
                {
                    CacheEntry.this.cache.writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                    this.onEnd.accept(EndFW.TYPE_ID, buffer, offset, toWrite);
                }
            }
        }
    }

    private ListFW<HttpHeaderFW> getCachedRequest()
    {
        return cachedRequest.getRequestHeaders(cache.cachedRequestHeadersRO, cache.cachedRequestBufferPool);
    }

    private ListFW<HttpHeaderFW> getCachedResponseHeaders()
    {
        return cachedRequest.getResponseHeaders(cache.cachedResponseHeadersRO, cache.cachedResponseBufferPool);
    }

    private void addClient()
    {
        this.clientCount++;
    }

    private void removeClient()
    {
        clientCount--;
        if (clientCount == 0 && this.state == CacheEntryState.PURGED)
        {
            cachedRequest.purge(cache.requestBufferPool); // Force hard clean up
        }
    }

    private boolean canBeServedToAuthorized(
        ListFW<HttpHeaderFW> request,
        short requestAuthScope)
    {

        if (SurrogateControl.isProtectedEx(this.getCachedResponseHeaders()))
        {
            return requestAuthScope == cachedRequest.authScope();
        }

        final CacheControl responseCacheControl = responseCacheControl();
        final ListFW<HttpHeaderFW> cachedRequestHeaders = this.getCachedRequest();
        return sameAuthorizationScope(request, cachedRequestHeaders, responseCacheControl);
    }

    private boolean doesNotVaryBy(ListFW<HttpHeaderFW> request)
    {
        final ListFW<HttpHeaderFW> responseHeaders = this.getCachedResponseHeaders();
        final ListFW<HttpHeaderFW> cachedRequest = getCachedRequest();
        return CacheUtils.doesNotVary(request, responseHeaders, cachedRequest);
    }


    private boolean satisfiesFreshnessRequirementsOf(
            ListFW<HttpHeaderFW> request,
            Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControlFW.parse(requestCacheControlHeaderValue);

        Instant staleAt = staleAt();
        if (requestCacheControl.contains(MIN_FRESH))
        {
            final String minFresh = requestCacheControl.getValue(MIN_FRESH);
            if (! now.plusSeconds(parseInt(minFresh)).isBefore(staleAt))
            {
                return false;
            }
        }
        return true;
    }

    private boolean satisfiesStalenessRequirementsOf(
            ListFW<HttpHeaderFW> request,
            Instant now)
    {
        final String requestCacheControlHeacerValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControlFW.parse(requestCacheControlHeacerValue);

        Instant staleAt = staleAt();
        if (requestCacheControl.contains(MAX_STALE))
        {
            final String maxStale = requestCacheControl.getValue(MAX_STALE);
            final int maxStaleSec = (maxStale != null) ? parseInt(maxStale): MAX_VALUE;
            final Instant acceptable = staleAt.plusSeconds(maxStaleSec);
            if (now.isAfter(acceptable))
            {
                return false;
            }
        }
        else if (now.isAfter(staleAt))
        {
            return false;
        }

        return true;
    }

    private boolean satisfiesAgeRequirementsOf(
        ListFW<HttpHeaderFW> request,
        Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControlFW.parse(requestCacheControlHeaderValue);

        if (requestCacheControl.contains(MAX_AGE))
        {
            int requestMaxAge = parseInt(requestCacheControl.getValue(MAX_AGE));
            Instant receivedAt = responseReceivedAt();
            if (receivedAt.plusSeconds(requestMaxAge).isBefore(now))
            {
                return false;
            }
        }
        return true;
    }

    private Instant staleAt()
    {
        if (lazyInitiatedResponseStaleAt == null)
        {
            CacheControl cacheControl = responseCacheControl();
            Instant receivedAt = responseReceivedAt();
            int staleInSeconds = cacheControl.contains(S_MAXAGE) ?
                parseInt(cacheControl.getValue(S_MAXAGE))
                : cacheControl.contains(MAX_AGE) ?  parseInt(cacheControl.getValue(MAX_AGE)) : 0;
            int surrogateAge = SurrogateControl.getSurrogateAge(this.getCachedResponseHeaders());
            staleInSeconds = Math.max(staleInSeconds, surrogateAge);
            lazyInitiatedResponseStaleAt = receivedAt.plusSeconds(staleInSeconds);
        }
        return lazyInitiatedResponseStaleAt;
    }

    private Instant responseReceivedAt()
    {
        if (lazyInitiatedResponseReceivedAt == null)
        {
            final ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
            final String dateHeaderValue = getHeader(responseHeaders, HttpHeaders.DATE) != null ?
                    getHeader(responseHeaders, HttpHeaders.DATE) : getHeader(responseHeaders, HttpHeaders.LAST_MODIFIED);
            try
            {
                Date receivedDate = DATE_FORMAT.parse(dateHeaderValue);
                lazyInitiatedResponseReceivedAt = receivedDate.toInstant();
            }
            catch (Exception e)
            {
                lazyInitiatedResponseReceivedAt = Instant.EPOCH;
            }
        }
        return lazyInitiatedResponseReceivedAt;
    }


    private CacheControl responseCacheControl()
    {
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        String cacheControl = getHeader(responseHeaders, CACHE_CONTROL);
        return cache.responseCacheControlFW.parse(cacheControl);
    }


    public boolean canServeRequest(
        ListFW<HttpHeaderFW> request,
        short authScope)
    {
        if (this.state == CacheEntryState.PURGED)
        {
            return false;
        }
        Instant now = Instant.now();

        final boolean canBeServedToAuthorized = canBeServedToAuthorized(request, authScope);
        final boolean doesNotVaryBy = doesNotVaryBy(request);
        final boolean satisfiesFreshnessRequirements = satisfiesFreshnessRequirementsOf(request, now);
        final boolean satisfiesStalenessRequirements = satisfiesStalenessRequirementsOf(request, now)
                || this.state == CAN_REFRESH || this.state == REFRESHING;
        final boolean satisfiesAgeRequirements = satisfiesAgeRequirementsOf(request, now);
        return canBeServedToAuthorized &&
                doesNotVaryBy &&
                satisfiesFreshnessRequirements &&
                satisfiesStalenessRequirements &&
                satisfiesAgeRequirements;
    }

    private boolean isStale()
    {
        return Instant.now().isAfter(staleAt());
    }

    protected boolean isIntendedForSingleUser()
    {
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        if (SurrogateControl.isProtectedEx(responseHeaders))
        {
            return false;
        }
        else
        {
            // TODO pull out as utility of CacheUtils
            String cacheControl = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.CACHE_CONTROL);
            return cacheControl == null && cache.responseCacheControlFW.parse(cacheControl).contains(CacheDirectives.PRIVATE);
        }
    }

    public boolean isUpdateRequestForThisEntry(ListFW<HttpHeaderFW> requestHeaders)
    {
        return CacheUtils.isMatchByEtag(requestHeaders, this.cachedRequest.etag());
    }


    public boolean subscribeToUpdate(OnUpdateRequest onModificationRequest)
    {
        final boolean polling = this.state == REFRESHING || this.state == CAN_REFRESH;
        if (polling)
        {
            this.subscribers.add(onModificationRequest);
        }
        if (this.state == CacheEntryState.CAN_REFRESH)
        {
            pollBackend();
        }
        return polling;
    }


    public void subscribers(Consumer<OnUpdateRequest> consumer)
    {
        subscribers.stream().forEach(consumer);
        subscribers.clear();
    }


    public boolean isUpdatedBy(CacheableRequest request)
    {
        ListFW<HttpHeaderFW> responseHeadersRO = request.getResponseHeaders(cache.responseHeadersRO, cache.responseBufferPool);
        String status = HttpHeadersUtil.getHeader(responseHeadersRO, HttpHeaders.STATUS);
        boolean updatedBy = false;
        if (!status.equals(HttpStatus.NOT_MODIFIED_304))
        {
            MutableDirectBuffer cachedResponsePayload = getCachedData();
            MutableDirectBuffer responsePayload = request.getData(cache.responseBufferPool);
            int cachedHeaderSize = cachedRequest.responseHeadersSize();
            int headerSize = request.responseHeadersSize();
            int cachedResponseSize = cachedRequest.responseSize();
            int responseSize = request.responseSize();

            int cachedPayloadSize = cachedResponseSize - cachedHeaderSize;
            int payloadSize = responseSize - headerSize;

            updatedBy = !DirectBufferUtil.equals(
                    cachedResponsePayload,
                    cachedHeaderSize,
                    cachedPayloadSize,
                    responsePayload,
                    headerSize,
                    payloadSize);
        }
        return updatedBy;
    }



    private MutableDirectBuffer getCachedData()
    {
        return this.cachedRequest.getData(cache.cachedResponseBufferPool);
    }

    public void refresh(AnswerableByCacheRequest request)
    {
        if (request == pollingRequest)
        {
            pollBackend();
        }
    }

    public boolean expectSubscribers()
    {
        return expectSubscribers || !subscribers.isEmpty();
    }

    public int requestUrl()
    {
        return this.cachedRequest.requestURLHash();
    }

}
