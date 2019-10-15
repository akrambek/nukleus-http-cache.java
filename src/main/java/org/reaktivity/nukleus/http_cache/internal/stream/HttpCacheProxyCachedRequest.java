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
package org.reaktivity.nukleus.http_cache.internal.stream;

import static java.lang.Math.min;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_ABORTED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_READY_SIGNAL;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyCachedRequest
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptReplyId;
    private final long acceptInitialId;
    private final int requestHash;
    private final int initialWindow;

    private int acceptReplyBudget;
    private long budgetId;
    private int padding;

    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;

    HttpCacheProxyCachedRequest(
        HttpCacheProxyFactory factory,
        int requestHash,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId,
        long acceptInitialId)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.acceptInitialId = acceptInitialId;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
    }

    void onAccept(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);
            onBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            onData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = factory.endRO.wrap(buffer, index, index + length);
            onEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
            onAbort(abort);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onWindow(window);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onSignal(signal);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onReset(reset);
            break;
        default:
            break;
        }
    }


    private void onBegin(
        BeginFW begin)
    {
        cacheEntry = factory.defaultCache.get(requestHash);
        cacheEntry.setSubscribers(1);
        // count all requests
        factory.counters.requests.getAsLong();
        factory.counters.requestsCacheable.getAsLong();

        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                begin.traceId(),
                                0L,
                                initialWindow,
                                0);

        factory.writer.doSignal(acceptRouteId,
                                acceptReplyId,
                                factory.supplyTraceId.getAsLong(),
                                CACHE_ENTRY_READY_SIGNAL);
    }

    private void onData(
        final DataFW data)
    {
        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                data.traceId(),
                                data.budgetId(),
                                data.reserved(),
                                0);
    }

    private void onEnd(
        final EndFW end)
    {
        //NOOP
    }

    private void onAbort(
        final AbortFW abort)
    {
        factory.writer.doSignal(acceptRouteId,
                                acceptReplyId,
                                factory.supplyTraceId.getAsLong(),
                                REQUEST_ABORTED_SIGNAL);

    }

    private void onSignal(
        SignalFW signal)
    {
        final int signalId = (int) signal.signalId();

        switch (signalId)
        {
        case CACHE_ENTRY_READY_SIGNAL:
            onServeCacheEntrySignal(signal);
            break;
        case REQUEST_ABORTED_SIGNAL:
            onRequestAbortedSignal(signal);
            break;
        default:
            break;
        }
    }

    private void onRequestAbortedSignal(SignalFW signal)
    {
        factory.writer.doAbort(acceptReply,
                               acceptRouteId,
                               acceptReplyId,
                               signal.traceId());
        cacheEntry.setSubscribers(-1);
    }

    private void onWindow(
        WindowFW window)
    {
        budgetId = window.budgetId();
        padding = window.padding();
        long streamId = window.streamId();
        int credit = window.credit();
        acceptReplyBudget += credit;
        factory.budgetManager.window(BudgetManager.StreamKind.CACHE,
                                     budgetId,
                                     streamId,
                                     credit,
                                     this::writePayload,
                                     window.traceId());
        sendEndIfNecessary(window.traceId());
    }

    private void onReset(
        ResetFW reset)
    {
        factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                     budgetId,
                                     acceptReplyId,
                                     this.factory.supplyTraceId.getAsLong());
        cacheEntry.setSubscribers(-1);
    }

    private void onServeCacheEntrySignal(
        SignalFW signal)
    {
        cacheEntry = factory.defaultCache.get(requestHash);
        sendHttpResponseHeaders(cacheEntry, signal.signalId());
    }

    private void sendHttpResponseHeaders(
        DefaultCacheEntry cacheEntry,
        long signalId)
    {
        ArrayFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        factory.writer.doHttpResponseWithUpdatedHeaders(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            responseHeaders,
            cacheEntry.getRequestHeaders(),
            cacheEntry.etag(),
            cacheEntry.isStale(),
            factory.supplyTraceId.getAsLong());


        payloadWritten = 0;

        factory.counters.responses.getAsLong();
        factory.defaultCache.counters.responsesCached.getAsLong();
    }

    private void sendEndIfNecessary(
        long traceId)
    {

        boolean ackedBudget = !factory.budgetManager.hasUnackedBudget(budgetId, acceptReplyId);

        if (payloadWritten == cacheEntry.responseSize() &&
            ackedBudget)
        {
            factory.writer.doHttpEnd(acceptReply,
                                     acceptRouteId,
                                     acceptReplyId,
                                     traceId);

            factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                                   budgetId,
                                                   acceptReplyId,
                                                   traceId);
            cacheEntry.setSubscribers(-1);
        }
    }

    private int writePayload(
        int budget,
        long trace)
    {
        final int minBudget = min(budget, acceptReplyBudget);
        final int toWrite = min(minBudget - padding, cacheEntry.responseSize() - payloadWritten);
        if (toWrite > 0)
        {
            factory.writer.doHttpData(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                trace,
                budgetId,
                toWrite + padding,
                p -> buildResponsePayload(payloadWritten,
                                          toWrite,
                                          p,
                                          factory.defaultCache.getResponsePool()));
            payloadWritten += toWrite;
            budget -= toWrite + padding;
            acceptReplyBudget -= toWrite + padding;
            assert acceptReplyBudget >= 0;
        }

        return budget;
    }

    private void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder p,
        BufferPool bp)
    {
        final int slotCapacity = bp.slotCapacity();
        final int startSlot = Math.floorDiv(index, slotCapacity) + NUM_OF_HEADER_SLOTS;
        buildResponsePayload(index, length, p, bp, startSlot);
    }

    private void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder builder,
        BufferPool bp,
        int slotCnt)
    {
        if (length == 0)
        {
            return;
        }

        final int slotCapacity = bp.slotCapacity();
        int chunkedWrite = (slotCnt * slotCapacity) - index;
        int slot = cacheEntry.getResponseSlots().get(slotCnt);
        if (chunkedWrite > 0)
        {
            MutableDirectBuffer buffer = bp.buffer(slot);
            int offset = slotCapacity - chunkedWrite;
            int chunkLength = Math.min(chunkedWrite, length);
            builder.put(buffer, offset, chunkLength);
            index += chunkLength;
            length -= chunkLength;
        }
        buildResponsePayload(index, length, builder, bp, ++slotCnt);
    }
}
