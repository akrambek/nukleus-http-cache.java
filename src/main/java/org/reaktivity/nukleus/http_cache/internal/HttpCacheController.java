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
package org.reaktivity.nukleus.http_cache.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.types.control.Role;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.UnrouteFW;

public final class HttpCacheController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: HttpPushConfiguration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();

    private final ControllerSpi controllerSpi;
    private final MutableDirectBuffer writeBuffer;

    public HttpCacheController(
        ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.writeBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<HttpCacheController> kind()
    {
        return HttpCacheController.class;
    }

    @Override
    public String name()
    {
        return "http-cache";
    }

    public <T> T supplySource(
        String source,
        BiFunction<MessagePredicate, ToIntFunction<MessageConsumer>, T> factory)
    {
        return controllerSpi.doSupplySource(source, factory);
    }

    public <T> T supplyTarget(
        String target,
        BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, T> factory)
    {
        return controllerSpi.doSupplyTarget(target, factory);
    }

    public CompletableFuture<Long> routeServer(
        String source,
        long sourceRef,
        String target,
        long targetRef)
    {

        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW route = routeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .correlationId(correlationId)
                .role(b -> b.set(Role.SERVER))
                .source(source)
                .sourceRef(sourceRef)
                .target(target)
                .targetRef(targetRef)
                .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    public CompletableFuture<Void> unrouteServer(
            String source,
            long sourceRef,
            String target,
            long targetRef)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unroute = unrouteRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .correlationId(correlationId)
                                     .role(b -> b.set(Role.SERVER))
                                     .source(source)
                                     .sourceRef(sourceRef)
                                     .target(target)
                                     .targetRef(targetRef)
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    public CompletableFuture<Long> routeProxy(
        String source,
        long sourceRef,
        String target,
        long targetRef)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW route = routeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .correlationId(correlationId)
                .role(b -> b.set(Role.PROXY))
                .source(source)
                .sourceRef(sourceRef)
                .target(target)
                .targetRef(targetRef)
                .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    public CompletableFuture<Void> unrouteProxy(
        String source,
        long sourceRef,
        String target,
        long targetRef)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unroute = unrouteRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .correlationId(correlationId)
                                     .role(b -> b.set(Role.PROXY))
                                     .source(source)
                                     .sourceRef(sourceRef)
                                     .target(target)
                                     .targetRef(targetRef)
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    public long count(String name)
    {
        return controllerSpi.doCount(name);
    }
}
