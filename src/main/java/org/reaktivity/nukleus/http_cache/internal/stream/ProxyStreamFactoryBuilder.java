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

import java.util.Random;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public class ProxyStreamFactoryBuilder implements StreamFactoryBuilder
{

    private final HttpCacheConfiguration config;
    private final LongObjectBiConsumer<Runnable> scheduler;
    private final Long2ObjectHashMap<Request> correlations;

    private RouteManager router;
    private MutableDirectBuffer writeBuffer;
    private LongSupplier supplyStreamId;
    private LongSupplier supplyCorrelationId;
    private Supplier<BufferPool> supplyBufferPool;
    private Slab bufferPool;
    private Cache cache;

    private int etagCnt = 0;
    private final int etagPrefix = new Random().nextInt(99999);
    final Supplier<String> supplyEtag = () ->
    {
        return "\"" + etagPrefix + "a" + etagCnt++ + "\"";
    };
    private LongSupplier entryAcquires;
    private LongSupplier entryReleases;

    public ProxyStreamFactoryBuilder(
            HttpCacheConfiguration config,
            LongObjectBiConsumer<Runnable> scheduler)
    {
        this.config = config;
        this.correlations = new Long2ObjectHashMap<>();
        this.scheduler = scheduler;
    }

    @Override
    public ProxyStreamFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setStreamIdSupplier(
        LongSupplier supplyStreamId)
    {
        this.supplyStreamId = supplyStreamId;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setCorrelationIdSupplier(
        LongSupplier supplyCorrelationId)
    {
        this.supplyCorrelationId = supplyCorrelationId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public StreamFactoryBuilder setCounterSupplier(
        Function<String, LongSupplier> supplyCounter)
    {
        entryAcquires = supplyCounter.apply("entry.acquires");
        entryReleases = supplyCounter.apply("entry.releases");
        return this;
    }

    @Override
    public StreamFactory build()
    {
        if (cache == null)
        {
            final int slotCapacity = supplyBufferPool.get().slotCapacity();
            final int httpCacheCapacity = config.httpCacheCapacity();
            this.bufferPool = new Slab(httpCacheCapacity, slotCapacity, entryAcquires, entryReleases);

            this.cache = new Cache(
                    scheduler,
                    writeBuffer,
                    bufferPool,
                    correlations,
                    supplyEtag);
        }
        return new ProxyStreamFactory(
                router,
                writeBuffer,
                bufferPool,
                supplyStreamId,
                supplyCorrelationId,
                correlations,
                scheduler,
                cache,
                supplyEtag);
    }
}
