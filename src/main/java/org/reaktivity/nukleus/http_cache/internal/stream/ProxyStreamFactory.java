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
package org.reaktivity.nukleus.http_cache.internal.stream;

import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.canBeServedByCache;
import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.responseCanSatisfyRequest;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.CACHE_SYNC;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.INJECTED_DEFAULT_HEADER;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE_VALUE;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.INJECTED_HEADER_DEFAULT_VALUE;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.INJECTED_HEADER_NAME;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.IS_POLL_HEADER;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.NO_CACHE_CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.cacheableResponse;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getRequestURL;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.internal.stream.Cache.CacheResponseServer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http_cache.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{

    private static final String CACHE_CONTROL = "cache-control";
    private static final String CACHE_SYNC_ALWAYS = "always";
    private static final String IF_UNMODIFIED_SINCE = "if-unmodified-since";
    private static final String IF_MATCH = "if-match";
    private static final String NO_CACHE = "no-cache";
    private static final String IF_NONE_MATCH = "if-none-match";
    private static final String IF_MODIFIED_SINCE = "if-modified-since";
    private static final String STALE_WHILE_REVALIDATE_2147483648 = "stale-while-revalidate=2147483648";

    // TODO, remove need for RW in simplification of inject headers
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> myRequestHeadersRO = new HttpBeginExFW().headers();
    private final ListFW<HttpHeaderFW> pendingRequestHeadersRO = new HttpBeginExFW().headers();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final RouteFW routeRO = new RouteFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final AbortFW abortRO = new AbortFW();

    private final RouteHandler router;

    private final LongSupplier supplyStreamId;
    private final BufferPool streamBufferPool;
    private final BufferPool correlationBufferPool;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final LongSupplier supplyCorrelationId;
    private final LongObjectBiConsumer<Runnable> scheduler;

    private final Writer writer;
    private final Long2ObjectHashMap<FanOut> junctions;

    private final BufferPool cacheBufferPool;

    private final Cache cache;

    public ProxyStreamFactory(
        RouteHandler router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations,
        LongObjectBiConsumer<Runnable> scheduler,
        Cache cache)
    {
        this.router = requireNonNull(router);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.streamBufferPool = requireNonNull(bufferPool);
        this.correlationBufferPool = bufferPool.duplicate();
        this.cacheBufferPool = bufferPool.duplicate();
        this.correlations = requireNonNull(correlations);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.scheduler = requireNonNull(scheduler);
        this.cache = cache;

        this.writer = new Writer(writeBuffer);
        this.junctions = new Long2ObjectHashMap<>();
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
            final BeginFW begin,
            final MessageConsumer networkThrottle)
    {
        final long networkRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return networkRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ProxyAcceptStream(networkThrottle, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
            final BeginFW begin,
            final MessageConsumer throttle)
    {
        final long throttleId = begin.streamId();

        return new ProxyConnectReplyStream(throttle, throttleId)::handleStream;
    }

    public final class ProxyAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private String acceptName;
        private MessageConsumer acceptReply;
        private long acceptReplyStreamId;
        private final long acceptStreamId;
        private long acceptCorrelationId;

        private MessageConsumer connect;
        private String connectName;
        private long connectRef;
        private long connectCorrelationId;
        private long connectStreamId;

        private MessageConsumer streamState;

        private int myRequestSlot = NO_SLOT;
        private int requestSize;
        private Correlation streamCorrelation;
        private MessageConsumer connectReplyThrottle;
        private FanOut junction;
        private int requestURLHash;
        private long connectReplyStreamId;

        private ProxyAcceptStream(
                MessageConsumer acceptThrottle,
                long acceptStreamId)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptStreamId = acceptStreamId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                this.acceptName = begin.source().asString();
                handleBegin(begin);
            }
            else
            {
                writer.doReset(acceptThrottle, acceptStreamId);
            }
        }

        private void handleBegin(BeginFW begin)
        {
            final long acceptRef = beginRO.sourceRef();
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);

            if (connectRoute == null)
            {
                // just reset
                writer.doReset(acceptThrottle, acceptStreamId);
            }
            else
            {
                this.connectName = connectRoute.target().asString();
                this.connect = router.supplyTarget(connectName);
                this.connectRef = connectRoute.targetRef();
                this.connectCorrelationId = supplyCorrelationId.getAsLong();

                this.acceptReply = router.supplyTarget(acceptName);
                this.acceptReplyStreamId = supplyStreamId.getAsLong();
                this.acceptCorrelationId = begin.correlationId();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

                // Should already be canonicalized in http / http2 nuklei
                final String requestURL = getRequestURL(requestHeaders);

                this.requestURLHash = requestURL.hashCode();

                if (!requestHeaders.anyMatch(
                        h ->
                        {
                            String name = h.name().asString();
                            return  "x-poll-injected".equals(name) ||
                                    "x-http-cache-sync".equals(name);
                        }))
                {
                    handleClientInitiatedRequest(requestHeaders, requestURL);
                }
                else if (hasOutstandingRequestThatMaySatisfy(requestHeaders, requestURLHash))
                {
                    latchOnToFanout(requestHeaders);
                }
                else
                {
                    fanout(requestHeaders, true);
                }
            }
        }

        private void handleClientInitiatedRequest(
                final ListFW<HttpHeaderFW> requestHeaders,
                final String requestURL)
        {
            if (canBeServedByCache(requestHeaders))
            {
                handleCacheableRequest(requestHeaders, requestURL);
            }
            else
            {
                proxyRequest(requestHeaders);
            }
        }

        private void handleCacheableRequest(
                final ListFW<HttpHeaderFW> requestHeaders,
                final String requestURL)
        {
            CacheResponseServer responseServer = cache.hasStoredResponseThatSatisfies(requestURLHash, requestHeaders);
            if (responseServer != null)
            {
                this.myRequestSlot = streamBufferPool.acquire(acceptStreamId);
                if (myRequestSlot == NO_SLOT)
                {
                    send503AndReset();
                    return;
                }
                storeRequest(requestHeaders, myRequestSlot);
                this.streamCorrelation =
                        new Correlation(
                            requestURLHash,
                            this::handleResponseFromProxy,
                            null,
                            NO_SLOT,
                            -1,
                            false,
                            requestURL,
                            connectRef);
                responseServer.serverClient(streamCorrelation);
                this.streamState = this::waitingForOutstanding;
            }
            else
            {
                fanout(requestHeaders, false);
            }
        }

        private void proxyRequest(
                final ListFW<HttpHeaderFW> requestHeaders)
        {
            this.myRequestSlot = streamBufferPool.acquire(acceptStreamId);
            if (myRequestSlot == NO_SLOT)
            {
                send503AndReset();
                return;
            }

            storeRequest(requestHeaders, myRequestSlot);
            this.connectStreamId = supplyStreamId.getAsLong();
            this.streamCorrelation = new Correlation(
                    requestURLHash,
                    this::handleResponseFromProxy,
                    null,
                    NO_SLOT,
                    -1,
                    false,
                    connectName,
                    connectRef);

            correlations.put(connectCorrelationId, streamCorrelation);

            this.connectStreamId = supplyStreamId.getAsLong();

            writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                requestHeaders.forEach(h ->
                    e.item(h2 -> h2.representation((byte) 0).name(h.name())
                                .value(h.value()))
                )
            );

            router.setThrottle(connectName, connectStreamId, this::handleConnectThrottle);
            this.streamState = this::afterProxyBegin;
        }

        private void fanout(
            final ListFW<HttpHeaderFW> requestHeaders,
            final boolean follow304)
        {
            // create connection
            this.myRequestSlot = streamBufferPool.acquire(acceptStreamId);
            if (myRequestSlot == NO_SLOT)
            {
                send503AndReset();
                return;
            }
            storeRequest(requestHeaders, myRequestSlot);

            int correlationRequestHeadersSlot = streamBufferPool.acquire(requestURLHash);
            if (correlationRequestHeadersSlot == NO_SLOT)
            {
                streamBufferPool.release(myRequestSlot);
                send503AndReset();
                return;
            }

            this.connectStreamId = supplyStreamId.getAsLong();
            storeRequest(requestHeaders, correlationRequestHeadersSlot);

            this.junction = new FanOut();

            Correlation correlation = new Correlation(
                requestURLHash,
                junction,
                correlationBufferPool,
                correlationRequestHeadersSlot,
                requestSize,
                follow304,
                connectName,
                connectRef);

            correlations.put(connectCorrelationId, correlation);

            junction.setStreamCorrelation(correlation);
            long2ObjectPutIfAbsent(junctions, requestURLHash, junction);
            junction.addSubscriber(this::handleResponseFromMyInitiatedFanout);

            String pollTime = getHeader(requestHeaders, "x-retry-after");
            if (pollTime != null)
            {
                scheduler.accept(currentTimeMillis() + parseInt(pollTime) * 1000, () ->
                {
                    if (!junction.getOuts().isEmpty())
                    {
                        final ListFW<HttpHeaderFW> myRequestHeaders = getRequestHeaders(myRequestHeadersRO);
                        sendRequest(connect, connectStreamId, connectRef, connectCorrelationId, myRequestHeaders);
                    }
                    else
                    {
                        correlations.remove(connectCorrelationId).cleanUp();
                    }
                });
            }
            else
            {
                sendRequest(connect, connectStreamId, connectRef, connectCorrelationId, requestHeaders);
            }

            // send 0 window back to complete handshake

            this.streamState = this::waitingForOutstanding;
        }

        private void latchOnToFanout(final ListFW<HttpHeaderFW> requestHeaders)
        {
            this.myRequestSlot = streamBufferPool.acquire(acceptStreamId);
            if (myRequestSlot == NO_SLOT)
            {
                send503AndReset();
                return;
            }
            storeRequest(requestHeaders, myRequestSlot);

            this.junction = junctions.get(requestURLHash);
            junction.addSubscriber(this::handleResponseFromFanout);

            // send 0 window back to complete handshake

            this.streamState = this::waitingForOutstanding;
        }

        private void send503AndReset()
        {
            writer.doReset(acceptThrottle, acceptStreamId);
            writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
            e.item(h -> h.representation((byte) 0)
                    .name(":status")
                    .value("503")));
            writer.doAbort(acceptReply, acceptReplyStreamId);
        }

        private boolean handleResponseFromMyInitiatedFanout(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch(msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
                    final ListFW<HttpHeaderFW> myRequestHeaders = getRequestHeaders(myRequestHeadersRO);
                    sendHttpResponse(responseHeaders, myRequestHeaders);
                    router.setThrottle(acceptName, acceptReplyStreamId, junction.getHandleAcceptReplyThrottle());
                    break;
                default:
                    this.proxyBackAfterBegin(msgTypeId, buffer, index, length);
            }
            return true;
        }

        private boolean handleResponseFromFanout(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();

                    this.streamCorrelation = this.junction.getStreamCorrelation();

                    ListFW<HttpHeaderFW> pendingRequestHeaders = streamCorrelation.headers(pendingRequestHeadersRO);

                    ListFW<HttpHeaderFW> myRequestHeaders = getRequestHeaders(myRequestHeadersRO);

                    if (responseCanSatisfyRequest(pendingRequestHeaders, myRequestHeaders, responseHeaders))
                    {
                        sendHttpResponse(responseHeaders, myRequestHeaders);
                        router.setThrottle(acceptName, acceptReplyStreamId, junction.getHandleAcceptReplyThrottle());
                        return true;
                    }
                    else
                    {
                        this.junction = null;
                        this.connectCorrelationId = supplyCorrelationId.getAsLong();

                        this.streamCorrelation = new Correlation(
                                requestURLHash,
                                this::handleResponseFromProxy,
                                null,
                                NO_SLOT,
                                -1,
                                false,
                                connectName,
                                connectRef);

                        correlations.put(connectCorrelationId, streamCorrelation);

                        this.connectStreamId = supplyStreamId.getAsLong();

                        final ListFW<HttpHeaderFW> requestHeaders = myRequestHeaders;
                        sendRequest(connect, connectStreamId, connectRef, connectCorrelationId, requestHeaders);
                        return false;
                    }
                default:
                    proxyBackAfterBegin(msgTypeId, buffer, index, length);
                    return true;
            }
        }

        private void handleResponseFromProxy(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
                    final ListFW<HttpHeaderFW> requestHeaders = getRequestHeaders(myRequestHeadersRO);
                    sendHttpResponse(responseHeaders, requestHeaders);
                    this.connectReplyStreamId = streamCorrelation.getConnectReplyStreamId();
                    this.connectReplyThrottle = streamCorrelation.connectReplyThrottle();
                    router.setThrottle(acceptName, acceptReplyStreamId, this::handleAcceptReplyThrottle);

                    break;
                default:
                    proxyBackAfterBegin(msgTypeId, buffer, index, length);
                    break;
            }
        }

        private void sendHttpResponse(
                final ListFW<HttpHeaderFW> responseHeaders,
                final ListFW<HttpHeaderFW> requestHeaders)
        {
            if (requestHeaders.anyMatch(IS_POLL_HEADER))
            {

                if (responseHeaders.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString())))
                {
                    writer.doHttpBegin2(acceptReply, acceptReplyStreamId, 0L,
                            acceptCorrelationId, appendStaleWhileRevalidate(responseHeaders));
                }
                else
                {
                    writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L,
                            acceptCorrelationId, injectStaleWhileRevalidate(headersToExtensions(responseHeaders)));
                }
                injectPushPromise(requestHeaders, responseHeaders);
            }
            else
            {
                writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                responseHeaders.forEach(h -> e.item(
                        h2 -> h2.representation((byte) 0)
                        .name(h.name())
                        .value(h.value())))
                        );
            }
        }

        private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> appendStaleWhileRevalidate(
                ListFW<HttpHeaderFW> headersFW)
        {
            return x -> headersFW
                    .forEach(h ->
                        {
                            final StringFW nameRO = h.name();
                            final String16FW valueRO = h.value();
                            final String name = nameRO.asString();
                            final String value = valueRO.asString();
                            if (CACHE_CONTROL.equals(name) && !value.contains(STALE_WHILE_REVALIDATE_2147483648))
                            {
                                x.item(y -> y.representation((byte) 0)
                                        .name(nameRO)
                                        .value(value + ", " + STALE_WHILE_REVALIDATE_2147483648));
                            }
                            else
                            {
                                x.item(y -> y.representation((byte) 0).name(nameRO).value(h.value()));
                            }
                        }
                    );
        }

        private Flyweight.Builder.Visitor injectStaleWhileRevalidate(
            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
        {
            mutator = mutator.andThen(
                    x ->  x.item(h -> h.representation((byte) 0).name("cache-control").value(STALE_WHILE_REVALIDATE_2147483648))
                );
            return visitHttpBeginEx(mutator);
        }

        private Flyweight.Builder.Visitor visitHttpBeginEx(
                Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)

        {
            return (buffer, offset, limit) ->
            httpBeginExRW.wrap(buffer, offset, limit)
                         .headers(headers)
                         .build()
                         .sizeof();
        }

        private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headersToExtensions(
                ListFW<HttpHeaderFW> headersFW)
        {
            return x -> headersFW
                    .forEach(h ->
                    x.item(y -> y.representation((byte) 0)
                            .name(h.name())
                            .value(h.value())));
        }

        private void injectPushPromise(
            ListFW<HttpHeaderFW> requestHeaders,
            ListFW<HttpHeaderFW> responseHeaders)
        {
            writer.doH2PushPromise(
                    acceptReply,
                    acceptReplyStreamId,
                    setPushPromiseHeaders(requestHeaders, responseHeaders));
        }

        private void proxyBackAfterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case DataFW.TYPE_ID:
                    dataRO.wrap(buffer, index, index + length);
                    OctetsFW payload = dataRO.payload();
                    writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
                    break;
                case EndFW.TYPE_ID:
                    writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                    clean();
                    break;
                case AbortFW.TYPE_ID:
                    writer.doAbort(acceptReply, acceptReplyStreamId);
                    clean();
                    break;
                default:
                    break;
            }
        }

        private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setPushPromiseHeaders(
                ListFW<HttpHeaderFW> requestHeadersFW,
                ListFW<HttpHeaderFW> responseHeadersFW)
        {
            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> result;

            if (requestHeadersFW.anyMatch(INJECTED_DEFAULT_HEADER) || requestHeadersFW.anyMatch(INJECTED_HEADER_AND_NO_CACHE))
            {
               result = x -> addRequestHeaders(requestHeadersFW, responseHeadersFW, x);
            }
            else if (requestHeadersFW.anyMatch(NO_CACHE_CACHE_CONTROL))
            {
                result = x ->
                {
                    addRequestHeaders(requestHeadersFW, responseHeadersFW, x);

                    x.item(y -> y.representation((byte) 0)
                            .name(INJECTED_HEADER_NAME)
                            .value(INJECTED_HEADER_DEFAULT_VALUE));

                    x.item(y -> y.representation((byte) 0)
                            .name(CACHE_SYNC)
                            .value(CACHE_SYNC_ALWAYS));
                };
            }
            else if (requestHeadersFW.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString())))
            {
                result = x ->
                {
                    addRequestHeaders(requestHeadersFW, responseHeadersFW, x);

                    x.item(y -> y.representation((byte) 0)
                                .name(INJECTED_HEADER_NAME)
                                .value(INJECTED_HEADER_AND_NO_CACHE_VALUE));

                    x.item(y -> y.representation((byte) 0)
                                .name(CACHE_SYNC)
                                .value(CACHE_SYNC_ALWAYS));
                };
            }
            else
            {
                result = x ->
                {
                    addRequestHeaders(requestHeadersFW, responseHeadersFW, x);

                    x.item(y -> y.representation((byte) 0)
                                .name(INJECTED_HEADER_NAME)
                                .value(INJECTED_HEADER_AND_NO_CACHE_VALUE));

                    x.item(y -> y.representation((byte) 0)
                            .name(CACHE_CONTROL)
                            .value(NO_CACHE));

                    x.item(y -> y.representation((byte) 0)
                                .name(CACHE_SYNC)
                                .value(CACHE_SYNC_ALWAYS));
                };
            }
            return result;
        }

        private void addRequestHeaders(
                ListFW<HttpHeaderFW> requestHeadersFW,
                ListFW<HttpHeaderFW> responseHeadersFW,
                Builder<HttpHeaderFW.Builder, HttpHeaderFW> x)
        {
            requestHeadersFW
               .forEach(h ->
               {
                   final StringFW nameFW = h.name();
                   final String name = nameFW.asString();
                   final String16FW valueFW = h.value();
                   final String value = valueFW.asString();

                   switch(name)
                   {
                       case CACHE_CONTROL:
                           if (value.contains(NO_CACHE))
                           {
                               x.item(y -> y.representation((byte) 0)
                                                .name(nameFW)
                                                .value(valueFW));
                           }
                           else
                           {
                               x.item(y -> y.representation((byte) 0)
                                                .name(nameFW)
                                                .value(value + ", no-cache"));
                           }
                           break;
                        case IF_MODIFIED_SINCE:
                           if (responseHeadersFW.anyMatch(h2 -> "last-modified".equals(h2.name().asString())))
                           {
                               final String newValue = getHeader(responseHeadersFW, "last-modified");
                               x.item(y -> y.representation((byte) 0)
                                            .name(nameFW)
                                            .value(newValue));
                           }
                           break;
                        case IF_NONE_MATCH:
                           if (responseHeadersFW.anyMatch(h2 -> "etag".equals(h2.name().asString())))
                           {
                               final String newValue = getHeader(responseHeadersFW, "etag");
                               x.item(y -> y.representation((byte) 0)
                                            .name(nameFW)
                                            .value(newValue));
                           }
                           break;
                        case IF_MATCH:
                        case IF_UNMODIFIED_SINCE:
                            break;
                       default: x.item(y -> y.representation((byte) 0)
                                            .name(nameFW)
                                            .value(valueFW));
                   }
               });
        }

        private void clean()
        {
            if (this.myRequestSlot != NO_SLOT)
            {
                streamBufferPool.release(this.myRequestSlot);
            }
        }

        private ListFW<HttpHeaderFW> getRequestHeaders(ListFW<HttpHeaderFW> headersRO)
        {
            return headersRO.wrap(streamBufferPool.buffer(this.myRequestSlot), 0, requestSize);
        }

        private int storeRequest(final ListFW<HttpHeaderFW> headers, int requestCacheSlot)
        {
            this.requestSize = 0;
            MutableDirectBuffer requestCacheBuffer = streamBufferPool.buffer(requestCacheSlot);
            headers.forEach(h ->
            {
                requestCacheBuffer.putBytes(this.requestSize, h.buffer(), h.offset(), h.sizeof());
                this.requestSize += h.sizeof();
            });
            return this.requestSize;
        }

        private void afterProxyBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                writer.doReset(acceptThrottle, acceptStreamId);
                break;
            }
        }

        private void handleData(
                DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(connect, connectStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void handleEnd(
                EndFW end)
        {
            writer.doHttpEnd(connect, connectStreamId);
        }

        private void handleAbort(
                AbortFW abort)
        {
            writer.doAbort(connect, connectStreamId);
        }

        private void waitingForOutstanding(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                // TODO H2 late headers?? RFC might say can't affect caching, but
                // probably should still forward should expected request not match...
                break;
            case DataFW.TYPE_ID:
            default:
                if (junction != null)
                {
                    // needed because could be handleMyInitatedFanOut or handleFanout
                    // or it could already be processed in which case this doesn't mean
                    // much...
                    junction.unsubscribe(this::handleResponseFromMyInitiatedFanout);
                }
                writer.doReset(acceptThrottle, acceptStreamId);
                break;
            }
        }

        private boolean hasOutstandingRequestThatMaySatisfy(
            ListFW<HttpHeaderFW> requestHeaders,
            int requestURLHash)
        {
            if (junctions.containsKey(requestURLHash))
            {
                return requestHeaders.anyMatch(h ->
                {
                    final String name = h.name().asString();
                    final String value = h.value().asString();
                    return "x-http-cache-sync".equals(name) && "always".equals(value);
                });
            }
            return false;
        }

        private void handleAcceptReplyThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleAcceptReplyWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleAcceptReplyReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleAcceptReplyWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(connectReplyThrottle, this.connectReplyStreamId, bytes, frames);
        }

        private void handleAcceptReplyReset(
            ResetFW reset)
        {
            writer.doReset(connectReplyThrottle, this.connectReplyStreamId);
        }

        private void handleConnectThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleConnectWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleConnectReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleConnectWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(acceptThrottle, acceptStreamId, bytes, frames);
        }

        private void handleConnectReset(
            ResetFW reset)
        {
            writer.doReset(acceptThrottle, acceptStreamId);
        }

    }

    private final class ProxyConnectReplyStream
    {
        private MessageConsumer streamState;

        private final MessageConsumer connectReplyThrottle;
        private final long connectReplyStreamId;

        private ProxyConnectReplyStream(
                MessageConsumer connectReplyThrottle,
                long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyStreamId = connectReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                writer.doReset(connectReplyThrottle, connectReplyStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long connectReplyRef = begin.sourceRef();
            final long connectCorrelationId = begin.correlationId();

            final Correlation streamCorrelation = connectReplyRef == 0L ?
                correlations.remove(connectCorrelationId) : null;

            if (streamCorrelation != null)
            {
                if (streamCorrelation.follow304())
                {
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
                    final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.headers(pendingRequestHeadersRO);
                    if (responseHeaders.anyMatch(h -> ":status".equals(h.name().asString())
                                                && "304".equals(h.value().asString())) &&
                        requestHeaders.anyMatch(h -> "x-poll-injected".equals(h.name().asString()))
                        )
                    {
                        final String connectName = streamCorrelation.connectName();
                        final long connectRef = streamCorrelation.connectRef();
                        final MessageConsumer newConnect = router.supplyTarget(connectName);
                        final long newConnectStreamId = supplyStreamId.getAsLong();
                        final long newConnectCorrelationId = supplyCorrelationId.getAsLong();
                        sendRequest(newConnect, newConnectStreamId, connectRef, newConnectCorrelationId, requestHeaders);
                        correlations.put(newConnectCorrelationId, streamCorrelation);
                        streamState = this::ignoreRest;
                    }
                    else
                    {
                        forwardConnectReply(begin, streamCorrelation);
                    }
                }
                else
                {
                    forwardConnectReply(begin, streamCorrelation);
                }
            }
            else
            {
                writer.doReset(connectReplyThrottle, connectReplyStreamId);
            }
        }

        private void forwardConnectReply(BeginFW begin, final Correlation streamCorrelation)
        {
            streamCorrelation.setConnectReplyThrottle(connectReplyThrottle);
            streamCorrelation.connectReplyStreamId(connectReplyStreamId);
            final MessageConsumer consumer = streamCorrelation.consumer();
            consumer.accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
            streamState = consumer;
        }

        private void ignoreRest(int msgTypeId, DirectBuffer buffer, int index, int length)
        {
            // NOOP
        }
    }

    private final class FanOut implements MessageConsumer
    {

        private final Set<MessagePredicate> outs;
        private Correlation streamCorrelation;
        private GroupThrottle connectReplyThrottle;
        private long connectReplyStreamId;
        private boolean cacheResponse = true;
        private int cacheResponseSlot = NO_SLOT;
        private int cacheResponseSize;
        private int cacheResponseHeadersSize;

        FanOut()
        {
            this.outs = new HashSet<>();
        }

        public Set<MessagePredicate> getOuts()
        {
            return outs;
        }

        public MessageConsumer getHandleAcceptReplyThrottle()
        {
            return this::handleAcceptThrottle;
        }

        public void setStreamCorrelation(Correlation correlation)
        {
            this.streamCorrelation = correlation;
        }

        public Correlation getStreamCorrelation()
        {
            return this.streamCorrelation;
        }

        private void addSubscriber(MessagePredicate out)
        {
            outs.add(out);
        }

        private void unsubscribe(MessagePredicate out)
        {
            outs.remove(out);
        }

        @Override
        public void accept(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (this.cacheResponse)
            {
                switch (msgTypeId)
                {
                    case BeginFW.TYPE_ID:
                        BeginFW begin = beginRO.wrap(buffer, index, index + length);
                        this.cacheResponse = cache(begin);
                        break;
                    case DataFW.TYPE_ID:
                        DataFW data = dataRO.wrap(buffer, index, index + length);
                        this.cacheResponse = cache(data);
                        break;
                    case EndFW.TYPE_ID:
                        EndFW end = endRO.wrap(buffer, index, index + length);
                        this.cacheResponse = cache(end);
                        break;
                    case AbortFW.TYPE_ID:
                        AbortFW abort = abortRO.wrap(buffer, index, index + length);
                        this.cacheResponse = cache(abort);
                        break;
                }
            }
            else
            {
                switch (msgTypeId)
                {
                    case EndFW.TYPE_ID:
                    case AbortFW.TYPE_ID:
                        this.streamCorrelation.cleanUp();
                        break;
                    default:
                        break;
                }
            }

            for (Iterator<MessagePredicate> i = outs.iterator(); i.hasNext();)
            {
                MessagePredicate messageConsumer = i.next();
                if (!messageConsumer.test(msgTypeId, buffer, index, length))
                {
                    i.remove();
                }
                junctions.remove(this.streamCorrelation.requestURLHash());

            }
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final MessageConsumer connectReply = streamCorrelation.connectReplyThrottle();
                this.connectReplyStreamId = streamCorrelation.getConnectReplyStreamId();
                this.connectReplyThrottle = new GroupThrottle(outs.size(), writer, connectReply, connectReplyStreamId);
            }
        }

        private boolean cache(BeginFW begin)
        {
            final OctetsFW extension = begin.extension();
            final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
            final boolean isCacheable = cacheableResponse(responseHeaders);
            if (isCacheable)
            {
                this.cacheResponseSlot = cacheBufferPool.acquire(this.connectReplyStreamId);
                if (cacheResponseSlot == NO_SLOT)
                {
                    return false;
                }
                int sizeof = responseHeaders.sizeof();
                if (cacheResponseSize + sizeof > cacheBufferPool.slotCapacity())
                {
                    cacheBufferPool.release(this.cacheResponseSlot);
                    return false;
                }
                else
                {
                    MutableDirectBuffer buffer = cacheBufferPool.buffer(this.cacheResponseSlot);
                    buffer.putBytes(cacheResponseSize, responseHeaders.buffer(), responseHeaders.offset(), sizeof);
                    cacheResponseSize += sizeof;
                    this.cacheResponseHeadersSize = sizeof;
                    return true;
                }
            }
            else
            {
                return false;
            }
        }

        private boolean cache(DataFW data)
        {
            OctetsFW payload = data.payload();
            int sizeof = payload.sizeof();
            if (cacheResponseSize + sizeof + 4 > cacheBufferPool.slotCapacity())
            {
                cacheBufferPool.release(this.cacheResponseSlot);
                return false;
            }
            else
            {
                MutableDirectBuffer buffer = cacheBufferPool.buffer(this.cacheResponseSlot);
                buffer.putBytes(cacheResponseSize, payload.buffer(), payload.offset(), sizeof);
                cacheResponseSize += sizeof;
                return true;
            }
        }

        private boolean cache(EndFW end)
        {
            // TODO H2 end headers
            final int requestURLHash = this.streamCorrelation.requestURLHash();
            final int requestSlot = this.streamCorrelation.requestSlot();
            final int requestSize = this.streamCorrelation.requestSize();
            final int responseSlot = this.cacheResponseSlot;
            final int responseHeadersSize = this.cacheResponseHeadersSize;
            final int responseSize = this.cacheResponseSize;
            cache.put(requestURLHash, requestSlot, requestSize, responseSlot, responseHeadersSize, responseSize);
            return true;
        }

        private boolean cache(AbortFW abort)
        {
              cacheBufferPool.release(this.cacheResponseSlot);
              return false;
        }

        public void handleAcceptThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleConnectWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleConnectReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleConnectWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();
            final long streamId = window.streamId();
            connectReplyThrottle.processWindow(streamId, bytes, frames);
        }

        private void handleConnectReset(
            ResetFW reset)
        {
            final long streamId = reset.streamId();
            connectReplyThrottle.processReset(streamId);
        }
    }

    RouteFW resolveTarget(
            long sourceRef,
            String sourceName)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() && sourceName.equals(route.source().asString());
        };

        return router.resolve(filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private void sendRequest(
            final MessageConsumer connect,
            final long connectStreamId,
            final long connectRef,
            final long connectCorrelationId,
            final ListFW<HttpHeaderFW> requestHeaders)
    {
        boolean stripNoCacheValue = false;

        Predicate<HttpHeaderFW> isInjected = h -> INJECTED_HEADER_NAME.equals(h.name().asString());
        isInjected = isInjected.or(h -> "x-http-cache-sync".equals(h.name().asString()));
        if (requestHeaders.anyMatch(INJECTED_HEADER_AND_NO_CACHE) && requestHeaders.anyMatch(NO_CACHE_CACHE_CONTROL))
            {
                isInjected = isInjected.or(h -> CACHE_CONTROL.equals(h.name().asString()));
                if (requestHeaders.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString())))
                {
                    stripNoCacheValue = true;
                }
        }
        final Predicate<HttpHeaderFW> forwardHeader = isInjected.negate();
        final boolean stripNoCacheValue2 = stripNoCacheValue;

        writer.doHttpBegin2(
            connect,
            connectStreamId,
            connectRef,
            connectCorrelationId,
            hs -> requestHeaders.forEach(h ->
            {
                final StringFW nameRO = h.name();
                final String name = nameRO.asString();
                final String16FW valueRO = h.value();
                final String value = valueRO.asString();
                if (forwardHeader.test(h))
                {
                    hs.item(b ->
                        b.representation((byte) 0).name(nameRO).value(valueRO)
                    );
                }
                else if (stripNoCacheValue2 && CACHE_CONTROL.equals(name) && !"no-cache".equals(value))
                {
                    hs.item(b ->
                        {
                            String replaceFirst = value.replaceFirst(",?\\s*no-cache", "");
                            b.representation((byte) 0)
                                    .name(nameRO)
                                    .value(replaceFirst);
                        }
                    );
                }
        }));
        writer.doHttpEnd(connect, connectStreamId);
    }

    // TODO add to Long2ObjectHashMap#putIfAbsent.putIfAbsent without Boxing
    public static <T> T long2ObjectPutIfAbsent(
            Long2ObjectHashMap<T> map,
            int key,
            T value)
    {
        T old = map.get(key);
        if (old == null)
        {
            map.put(key, value);
        }
        return old;
    }
}