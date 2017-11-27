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

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class Request
{
    public enum Type
    {
        ON_UPDATE, PROXY, INITIAL_REQUEST, CACHE_REFRESH
    }

    final String acceptName;
    final MessageConsumer acceptReply;
    final long acceptReplyStreamId;
    final long acceptCorrelationId;
    final RouteManager router;

    public Request(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        RouteManager router)
    {
        this.acceptName = acceptName;
        this.acceptReply = acceptReply;
        this.acceptReplyStreamId = acceptReplyStreamId;
        this.acceptCorrelationId = acceptCorrelationId;
        this.router = router;
    }

    public abstract Type getType();

    public MessageConsumer acceptReply()
    {
        return acceptReply;
    }

    public long acceptReplyStreamId()
    {
        return acceptReplyStreamId;
    }

    public long acceptRef()
    {
        return 0L;
    }

    public long acceptCorrelationId()
    {
        return acceptCorrelationId;
    }

    public void setThrottle(MessageConsumer throttle)
    {
        this.router.setThrottle(acceptName, acceptReplyStreamId, throttle);
    }

    public abstract void purge(BufferPool bufferPool);

}
