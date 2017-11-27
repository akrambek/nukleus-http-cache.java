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
import static org.junit.rules.RuleChain.outerRule;

import java.time.Instant;

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

import org.junit.Assert;

public class EdgeArchProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/edge-arch");

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
        "${streams}/does.not.inject.on.post/accept/client",
        "${streams}/does.not.inject.on.post/connect/server",
        })
    public void shouldNotInjectOnPost() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.inject.on.non-cacheable.response/accept/client",
        "${streams}/does.not.inject.on.non-cacheable.response/connect/server",
    })
    public void shouldNotInjectOnNonCacheableResponse() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/serve.from.cache.when.freshness.extension.is.valid/accept/client",
        "${streams}/serve.from.cache.when.freshness.extension.is.valid/connect/server",
    })
    public void serveFromCacheWhenFreshnessExtensionIsValid() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/share.with.x-protected.scope/accept/client",
        "${streams}/share.with.x-protected.scope/connect/server",
    })
    public void shareWithXProtectedScope() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.share.with.different.protected.scope/accept/client",
        "${streams}/does.not.share.with.different.protected.scope/connect/server",
    })

    public void doesNotShareWithDifferentProtectedScope() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(2);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/freshness-extension.inject.individualized.push.promises/accept/client",
        "${streams}/freshness-extension.inject.individualized.push.promises/connect/server",
    })
    public void shouldInjectIndividualizedPushPromisesOnSharedFreshnessExtension() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1, 1, 1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/inject.stale-while-revalidate.push-promise.no-cache/accept/client",
        "${streams}/inject.stale-while-revalidate.push-promise.no-cache/connect/server",
    })
    public void shouldInjectValuesOnFreshnessExtension() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.and.poll.on.surrogate.max-age.when.fresh.ext/accept/client",
        "${streams}/cache.and.poll.on.surrogate.max-age.when.fresh.ext/connect/server",
    })
    public void shouldCacheAndPollOnSurrogateMaxAgeWhenFreshExt() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1, 1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.updates.cache/accept/client",
        "${streams}/polling.updates.cache/connect/server",
    })
    public void shouldUpdateCacheOnPoll() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CACHE_UPDATE_SENT");
        Thread.sleep(10);
        k3po.notifyBarrier("CACHE_UPDATE_RECEIVED");
        k3po.finish();
        Thread.sleep(1000);
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.waits.on.surrogate-age/accept/client",
        "${streams}/polling.waits.on.surrogate-age/connect/server",
    })
    public void pollingWaitsOnSurrogateAge() throws Exception
    {
        k3po.start();
        Instant start = Instant.now();
        k3po.awaitBarrier("CACHE_UPDATE_SENT");
        Thread.sleep(10);
        k3po.notifyBarrier("CACHE_UPDATE_RECEIVED");
        k3po.finish();
        Instant finish = Instant.now();
        Assert.assertTrue(start.plusMillis(4900).isBefore(finish));
        counters.assertExpectedCacheEntries(1, 1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.updates.pending.on-update.requests/accept/client",
        "${streams}/polling.updates.pending.on-update.requests/connect/server",
    })
    public void shouldUpdateOnUpdateRequestsWhenPollCompletes() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1, 1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.update.attachs.to.next.cache.if.push.promise.arrives.before.response.completes/accept/client",
        "${streams}/polling.update.attachs.to.next.cache.if.push.promise.arrives.before.response.completes/connect/server",
    })
    public void shouldAttachToNextCacheEntryIfPushPromiseArrivesBeforeResponseCompletes() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1, 1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.updates.pending.on-update.requests.only.when.modified/accept/client",
        "${streams}/polling.updates.pending.on-update.requests.only.when.modified/connect/server",
    })
    public void shouldUpdateOnUpdateRequestsOnlyWhenModified() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/failed.polling.aborts.pending.on-update.requests/accept/client",
        "${streams}/failed.polling.aborts.pending.on-update.requests/connect/server",
    })
    public void shouldAbortPendingOnUpdateRequestsWhenFailedPollingUpdates() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/failed.polling.aborts.pending.on-update.requests.and.recovers/accept/client",
        "${streams}/failed.polling.aborts.pending.on-update.requests.and.recovers/connect/server",
    })
    public void shouldAbortPendingOnUpdateRequestsWhenFailedPollingUpdatesAndRecovers() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.403.response.cancels.pending.on-update.requests/accept/client",
        "${streams}/polling.403.response.cancels.pending.on-update.requests/connect/server",
    })
    public void shouldCancelPushPromisesOn403() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.stops.if.no.subscribers/accept/client",
        "${streams}/polling.stops.if.no.subscribers/connect/server",
    })
    public void shouldStopPollingIfNoSubscribers() throws Exception
    {
        k3po.finish();
        Thread.sleep(10); // Wait for response to be processed
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/polling.stops.if.no.subscribers.and.not.updated/accept/client",
        "${streams}/polling.stops.if.no.subscribers.and.not.updated/connect/server",
    })
    public void shouldStopPollingIfNoSubscribersAndNotUpdated() throws Exception
    {
        k3po.finish();
        Thread.sleep(10); // Wait for response to be processed
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/maintain.polling.per.multiple.auth.scopes/accept/client",
        "${streams}/maintain.polling.per.multiple.auth.scopes/connect/server",
    })
    public void shouldMaintainPollingForMultipleAuthScopes() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(2, 0, 2);
    }
}
