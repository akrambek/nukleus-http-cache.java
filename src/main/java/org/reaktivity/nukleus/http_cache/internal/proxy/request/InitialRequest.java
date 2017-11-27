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

import java.util.function.LongSupplier;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public class InitialRequest extends CacheableRequest
{

    public InitialRequest(
            String acceptName,
            MessageConsumer acceptReply,
            long acceptReplyStreamId,
            long acceptCorrelationId,
            MessageConsumer connect,
            long connectRef,
            LongSupplier supplyCorrelationId,
            LongSupplier supplyStreamId,
            int requestURLHash,
            int requestSlot,
            int requestSize,
            RouteManager router,
            short authScope,
            String etag)
    {
        super(acceptName,
              acceptReply,
              acceptReplyStreamId,
              acceptCorrelationId,
              connect,
              connectRef,
              supplyCorrelationId,
              supplyStreamId,
              requestURLHash,
              requestSlot,
              requestSize,
              router,
              authScope,
              etag);
    }

    @Override
    public Type getType()
    {
        return Type.INITIAL_REQUEST;
    }

}
