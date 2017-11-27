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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public class Writer
{

    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer("http-cache".getBytes(UTF_8));

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    private final MutableDirectBuffer writeBuffer;
    private final BufferPool bufferPool;

    public Writer(
            MutableDirectBuffer writeBuffer,
            BufferPool bufferPool)
    {
        this.writeBuffer = writeBuffer;
        this.bufferPool = bufferPool;
    }

    public void doHttpBegin(
        MessageConsumer target,
        long targetStreamId,
        long targetRef,
        long correlationId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(targetStreamId)
                               .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                               .sourceRef(targetRef)
                               .correlationId(correlationId)
                               .extension(e -> e.set(visitHttpBeginEx(mutator)))
                               .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

    }

    public void doHttpResponseWithUpdatedCacheControl(
            MessageConsumer target,
            long targetStreamId,
            long targetRef,
            long correlationId,
            CacheControl cacheControlFW,
            ListFW<HttpHeaderFW> responseHeaders,
            int staleWhileRevalidate,
            String etag)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator =
                builder -> updateResponseHeaders(builder, cacheControlFW, responseHeaders, staleWhileRevalidate, etag);
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetStreamId)
                .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();
        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void updateResponseHeaders(
            Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
            CacheControl cacheControlFW,
            ListFW<HttpHeaderFW> responseHeadersRO,
            int staleWhileRevalidate,
            String etag)
    {
        responseHeadersRO.forEach(h ->
        {
            final StringFW nameFW = h.name();
            final String name = nameFW.asString();
            final String16FW valueFW = h.value();
            final String value = valueFW.asString();

            switch(name)
            {
                case HttpHeaders.CACHE_CONTROL:
                    cacheControlFW.parse(value);
                    if (cacheControlFW.contains("stale-while-revalidate"))
                    {
                        StringBuilder cacheControlDirectives = new StringBuilder();
                        cacheControlFW.getValues().entrySet().stream().forEach(e ->
                        {
                            String directive = e.getKey(); e.getValue();
                            String optionalValue = e.getValue();
                            if (cacheControlDirectives.length() > 0)
                            {
                                cacheControlDirectives.append(", ");
                            }
                            if ("stale-while-revalidate".equals(directive))
                            {
                                cacheControlDirectives.append("stale-while-revalidate=" + staleWhileRevalidate);
                            }
                            else
                            {
                                if (optionalValue == null)
                                {
                                    cacheControlDirectives.append(directive);
                                }
                                else
                                {
                                    cacheControlDirectives.append(directive + "=" + optionalValue);
                                }
                            }
                        });
                    }
                    else
                    {
                        builder.item(header -> header.name(nameFW).value(valueFW));
                    }
                    break;
                default: builder.item(header -> header.name(nameFW).value(valueFW));
            }
        });
        if (!responseHeadersRO.anyMatch(HAS_CACHE_CONTROL))
        {
            builder.item(header -> header.name("cache-control").value("stale-while-revalidate=" + staleWhileRevalidate));
        }
        if (!responseHeadersRO.anyMatch(h -> ETAG.equals(h.name().asString())))
        {
            builder.item(header -> header.name(ETAG).value(etag));
        }
    }

    public void doHttpData(
        MessageConsumer target,
        long targetStreamId,
        DirectBuffer payload,
        int offset,
        int length)
    {

        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .streamId(targetStreamId)
                            .payload(p -> p.set(payload, offset, length))
                            .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpEnd(
        MessageConsumer target,
        long targetStreamId)
    {
        EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                         .streamId(targetStreamId)
                         .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    public void doAbort(
            MessageConsumer target,
            long targetStreamId)
    {
        AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetStreamId)
                .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    public void doWindow(
        final MessageConsumer throttle,
        final long throttleStreamId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleStreamId)
                .credit(credit)
                .padding(padding)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    public void doReset(
        final MessageConsumer throttle,
        final long throttleStreamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(throttleStreamId)
                                     .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
                httpBeginExRW.wrap(buffer, offset, limit)
                             .headers(headers)
                             .build()
                             .sizeof();
    }

    public void doHttpPushPromise(
        AnswerableByCacheRequest request,
        ListFW<HttpHeaderFW> responseHeaders,
        int freshnessExtension,
        String etag)
    {
        final ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(requestHeadersRO, bufferPool);
        final MessageConsumer acceptReply = request.acceptReply();
        final long acceptReplyStreamId = request.acceptReplyStreamId();

        doH2PushPromise(
            acceptReply,
            acceptReplyStreamId,
            setPushPromiseHeaders(requestHeaders, responseHeaders, freshnessExtension, etag));
    }

    private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setPushPromiseHeaders(
            ListFW<HttpHeaderFW> requestHeadersRO,
            ListFW<HttpHeaderFW> responseHeadersRO,
            int freshnessExtension,
            String etag)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> result =
                builder -> updateRequestHeaders(requestHeadersRO, responseHeadersRO, builder, freshnessExtension, etag);

        return result;
    }

    private void updateRequestHeaders(
            ListFW<HttpHeaderFW> requestHeadersFW,
            ListFW<HttpHeaderFW> responseHeadersFW,
            Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
            int freshnessExtension,
            String etag)
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
                   case HttpHeaders.CACHE_CONTROL:
                       if (value.contains(CacheDirectives.NO_CACHE))
                       {
                           builder.item(header -> header.name(nameFW)
                                                        .value(valueFW));
                       }
                       else
                       {
                           builder.item(header -> header.name(nameFW)
                                                        .value(value + ", no-cache"));
                       }
                       break;
                    case HttpHeaders.IF_MODIFIED_SINCE:
                       if (responseHeadersFW.anyMatch(h2 -> "last-modified".equals(h2.name().asString())))
                       {
                           final String newValue = getHeader(responseHeadersFW, "last-modified");
                           builder.item(header -> header.name(nameFW)
                                                        .value(newValue));
                       }
                       break;
                    case HttpHeaders.IF_NONE_MATCH:
                       String result = etag;
                       if (responseHeadersFW.anyMatch(h2 -> "etag".equals(h2.name().asString())))
                       {
                           final String existingIfNoneMatch = getHeader(responseHeadersFW, "etag");
                           if (!existingIfNoneMatch.contains(etag))
                           {
                               result += ", " + existingIfNoneMatch;
                           }
                           else
                           {
                               result = existingIfNoneMatch;
                           }
                       }
                       final String finalEtag = result;
                       builder.item(header -> header.name(nameFW)
                               .value(finalEtag));
                       break;
                    case HttpHeaders.IF_MATCH:
                    case HttpHeaders.IF_UNMODIFIED_SINCE:
                        break;
                   default: builder.item(header -> header.name(nameFW)
                                                         .value(valueFW));
               }
           });
           if (!requestHeadersFW.anyMatch(HAS_CACHE_CONTROL))
           {
               builder.item(header -> header.name("cache-control").value("no-cache"));
           }
           if (!requestHeadersFW.anyMatch(PreferHeader.HAS_HEADER))
           {
               builder.item(header -> header.name("prefer").value("wait=" + freshnessExtension));
           }
           if (!requestHeadersFW.anyMatch(h -> HttpHeaders.IF_NONE_MATCH.equals(h.name().asString())))
           {
               builder.item(header -> header.name(IF_NONE_MATCH).value(etag));
           }
    }

    private void doH2PushPromise(
        MessageConsumer target,
        long targetId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .streamId(targetId)
            .payload(e -> {})
            .extension(e -> e.set(visitHttpBeginEx(mutator)))
            .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void do503AndAbort(
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId)
    {
        this.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
        e.item(h -> h.representation((byte) 0)
                .name(STATUS)
                .value("503")));
        this.doAbort(acceptReply, acceptReplyStreamId);
    }
}
