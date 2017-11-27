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

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
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

public class Rfc7234ProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/rfc7234");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

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
        "${streams}/proxy.get.request/accept/client",
        "${streams}/proxy.get.request/connect/server",
        })
    public void shouldProxyGetRequest() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/proxy.get.request.with.body/accept/client",
        "${streams}/proxy.get.request.with.body/connect/server",
        })
    public void shouldProxyGetRequestWithBody() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/proxy.get.request.with.transfer.encoding/accept/client",
        "${streams}/proxy.get.request.with.transfer.encoding/connect/server",
    })
    public void shouldProxyGetRequestWithTransferEncoding() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/proxy.post.request/accept/client",
        "${streams}/proxy.post.request/connect/server",
    })
    public void shouldProxyPostRequest() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }


    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/proxy.request.and.304/accept/client",
        "${streams}/proxy.request.and.304/connect/server",
    })
    public void shouldProxyRequestWith304() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.max-age/accept/client",
        "${streams}/cache.max-age/connect/server",
    })
    public void shouldCacheMaxAge() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.greater.max-age/accept/client",
            "${streams}/request.greater.max-age/connect/server",
    })
    public void shouldNotCacheWhenResponseAgeIsGreaterThanRequestMaxAge() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(2000);
        k3po.notifyBarrier("WAIT_2_SECONDS");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.lesser.max-age/accept/client",
            "${streams}/request.lesser.max-age/connect/server",
    })
    public void shouldCacheWhenResponseAgeIsLessthanRequestMaxAge() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_WAITS_1_SEC");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/cache.max-stale.with.value/accept/client",
            "${streams}/cache.max-stale.with.value/connect/server",
    })
    public void shouldCacheMaxStale() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.min-fresh/accept/client",
        "${streams}/cache.min-fresh/connect/server",
    })
    public void shouldCacheMinFresh() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_WAIT_1_SEC");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/cache.max-stale.no.value/accept/client",
            "${streams}/cache.max-stale.no.value/connect/server",
    })
    public void shouldCacheMaxStaleWithNoValue() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(2000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/cache.max-stale.with.max-age/accept/client",
            "${streams}/cache.max-stale.with.max-age/connect/server",
    })
    public void shouldCacheMaxStaleWithMaxAge() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/expire.max-stale/accept/client",
            "${streams}/expire.max-stale/connect/server",
    })
    public void shouldExpireMaxStale() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(2000);
        k3po.notifyBarrier("CACHE_EXPIRED_AND_STALE_FOR_2_SECONDS");
        k3po.finish();
        counters.assertExpectedCacheEntries(1); // NOTE lazy cache purge
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/expire.min-fresh/accept/client",
            "${streams}/expire.min-fresh/connect/server",
    })
    public void shouldExpireMinFresh() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.cache.max-age=0/accept/client",
            "${streams}/request.cache.max-age=0/connect/server",
    })
    public void shouldRequestCacheMaxAgeZero() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0); // In future this can change if we cache the entry
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.cache.max-age=0.and.304/accept/client",
            "${streams}/request.cache.max-age=0.and.304/connect/server",
    })
    public void shouldRequestCacheMaxAgeZeroAnd304() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0); // In future this can change if we cache the entry
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/cache.get.request.with.no-store/accept/client",
            "${streams}/cache.get.request.with.no-store/connect/server",
    })
    public void shouldCacheGetRequestWithNoStore() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Ignore("no-store is not implemented, need fix in ProxyAcceptStreamHandle begin" +
            "(can be served by cache but is not CacheableRequest)")
    @Specification({
            "${route}/proxy/controller",
            "${streams}/cache.get.request.with.no-store.and.response.marked.cacheable/accept/client",
            "${streams}/cache.get.request.with.no-store.and.response.marked.cacheable/connect/server",
    })
    public void shouldCacheGetRequestWithNoStoreAndResponeMarkedCacheable() throws Exception
    {
        k3po.start();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/expire.max-age/accept/client",
        "${streams}/expire.max-age/connect/server",
    })
    public void shouldExpireMaxAge() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.no-cache/accept/client",
            "${streams}/request.no-cache/connect/server",
    })
    public void shouldRequestNoCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.only-if-cached/accept/client",
            "${streams}/request.only-if-cached/connect/server",
    })
    public void shouldRequestOnlyIfCached() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Ignore("Can not guarantee race between RESET and ABORT")
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.only-if-cached.and.504/accept/client"
    })
    public void shouldRequestOnlyIfCachedAnd504() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/request.expire.only-if-cached/accept/client",
            "${streams}/request.expire.only-if-cached/connect/server",
    })
    public void shouldRequestExpireOnlyIfCached() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/should.bypass.cache.on.no.cache/accept/client",
        "${streams}/should.bypass.cache.on.no.cache/connect/server",
    })
    public void shouldBypassCacheOnNoCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.s-maxage/accept/client",
        "${streams}/cache.s-maxage/connect/server",
    })
    public void shouldCacheSMaxage() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/expire.s-maxage/accept/client",
        "${streams}/expire.s-maxage/connect/server",
    })
    public void shouldExpireSMaxage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(1000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/expire.cache.by.default.for.0.seconds/accept/client",
        "${streams}/expire.cache.by.default.for.0.seconds/connect/server",
    })
    public void shouldExpireCacheDefaultCacheableFor0Second() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(10);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.by.default.for.10.percent.of.last-modified/accept/client",
        "${streams}/cache.by.default.for.10.percent.of.last-modified/connect/server",
    })
    public void shouldCacheDefaultFor10PercentOfLastModified() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/expire.cache.by.default.for.10.percent.of.last-modified/accept/client",
        "${streams}/expire.cache.by.default.for.10.percent.of.last-modified/connect/server",
    })
    public void shouldExpireCacheDefaultFor10PercentOfLastModified() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(5000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/private.cache/accept/client",
        "${streams}/private.cache/connect/server",
    })
    public void shouldNotUsePrivateCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/implied.private.cache/accept/client",
        "${streams}/implied.private.cache/connect/server",
    })
    public void shouldNotUseImpliedPrivateCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/explicitly.public.cache/accept/client",
        "${streams}/explicitly.public.cache/connect/server",
    })
    public void shouldUseExplicitlyPublicCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/not.use.cache.that.varys/accept/client",
        "${streams}/not.use.cache.that.varys/connect/server",
    })
    public void shouldNotUseCacheForRequestThatVarys() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.that.varys.but.matches/accept/client",
        "${streams}/cache.that.varys.but.matches/connect/server",
    })
    public void shouldUseCacheForRequestThatMatchesVarys() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/proxy.response.too.large.to.cache/accept/client",
        "${streams}/proxy.response.too.large.to.cache/connect/server",
    })
    public void shouldProxyResponseTooLargeToCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/cache.if.server.returns.503.while.validation/accept/client",
        "${streams}/cache.if.server.returns.503.while.validation/connect/server",
    })
    public void shouldCacheIfServerReturns503WhileValidation() throws Exception
    {
        k3po.finish();
    }

    //
    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/invalidate.multiple.max-age/accept/client",
        "${streams}/invalidate.multiple.max-age/accept/server",
    })
    public void shouldNotCacheWithMultipleMaxAge() throws Exception
    {
        k3po.finish();
    }
    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/ignore.expires.if.response.contains.max-age/accept/client",
        "${streams}/ignore.expires.if.response.contains.max-age/connect/server",
    })
    public void shouldCacheMaxAgeAndExpires() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(3000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/ignore.expires.if.response.contains.s-maxage/accept/client",
            "${streams}/ignore.expires.if.response.contains.s-maxage/connect/server",
    })
    public void shouldCacheSMaxAgeWithExpires() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(3000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/response.no-cache/accept/client",
        "${streams}/response.no-cache/connect/server",
    })
    public void shouldRevalidateOnResponseNoCache() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/response.no-cache.with.max-stale/accept/client",
            "${streams}/response.no-cache.with.max-stale/connect/server",
    })
    public void shouldRevalidateOnResponseNoCacheWithStaleResponseConfigured() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(3000);
        k3po.notifyBarrier("CACHE_EXPIRED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/do.not.cache.response.with.no-store/accept/client",
        "${streams}/do.not.cache.response.with.no-store/connect/server",
    })
    public void shouldNotCacheResponseWithResponseNoStore() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/not.cache.when.authorization.is.provided/accept/client",
            "${streams}/not.cache.when.authorization.is.provided/connect/server",
    })
    public void shouldNotCacheWithRequestAuthorizationHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/not.use.cache.that.varys.with.asterisk.value/accept/client",
        "${streams}/not.use.cache.that.varys.with.asterisk.value/connect/server",
    })
    public void shouldNotUseCacheForRequestThatHasAsteriskSymbolValueInVary() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/not.cache.private.cache.with.s-maxage/accept/client",
            "${streams}/not.cache.private.cache.with.s-maxage/connect/server",
    })
    public void shouldNotCacheResponseWithSMaxageInPrivateCache() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/cache.with.freshened.response.that.updated.by.strong.validator/accept/client",
            "${streams}/cache.with.freshened.response.that.updated.by.strong.validator/connect/server",
    })
    public void shouldCacheWithFreshenedResponseThatUpdatedByStrongValidator() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
            "${streams}/override.max-age.with.s-maxage/accept/client",
            "${streams}/override.max-age.with.s-maxage/connect/server",
    })
    public void shouldOverrideMaxAgeWithSMaxage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_CACHED");
        sleep(3000);
        k3po.notifyBarrier("MAX_AGE_EXPIRED");
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/proxy/controller",
        "${streams}/explicitly.smaxage.and.authorization/accept/client",
        "${streams}/explicitly.smaxage.and.authorization/connect/server",
    })
    public void shouldCacheWithRequestAuthorizationHeaderAndSmaxage() throws Exception
    {
        k3po.finish();
    }
}
