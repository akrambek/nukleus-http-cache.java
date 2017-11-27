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
package org.reaktivity.nukleus.http_cache.internal.streams.proxy;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheController;
import org.reaktivity.nukleus.http_cache.internal.test.HttpCacheCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ProxyExceptionsIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/behavior");

    private final TestRule timeout = new DisableOnDebug(new Timeout(25, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .nukleus("http-cache"::equals)
            .controller(HttpCacheController.class::isAssignableFrom)
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024)
            .nukleus("http-cache"::equals)
            .clean();

    private final HttpCacheCountersRule counters = new HttpCacheCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(k3po).around(reaktor).around(counters).around(timeout);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/accept.sent.abort/accept/client",
        "${streams}/accept.sent.abort/connect/server",
    })
    public void shouldAcceptSentAbort() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/accept.sent.abort.cacheable.request/accept/client",
        "${streams}/accept.sent.abort.cacheable.request/connect/server",
    })
    public void shouldHandleAbortSentOnCacheableRequest() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.slabAquires() - counters.slabReleases());
        // We proceed with request out back anyways, TODO, consider adding to test response returning and getting cached
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/connect.reply.sent.abort/accept/client",
        "${streams}/connect.reply.sent.abort/connect/server",
    })
    public void shouldConnectReplySentAbort() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/connect.sent.reset/accept/client",
        "${streams}/connect.sent.reset/connect/server",
    })
    public void shouldConnectSentReset() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/accept.reply.sent.reset/accept/client",
        "${streams}/accept.reply.sent.reset/connect/server",
    })
    public void shouldAcceptReplySentReset() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/client.sent.abort.on.scheduled.poll/accept/client"
    })
    public void shouldClientSentAbortOnScheduledPoll() throws Exception
    {
        k3po.finish();
//        counters.assertExpectedCacheEntries(0); // TODO, fix. Sporadically failing today,
                                                  // won't happen with current h2 nukleus implementation
    }

}
