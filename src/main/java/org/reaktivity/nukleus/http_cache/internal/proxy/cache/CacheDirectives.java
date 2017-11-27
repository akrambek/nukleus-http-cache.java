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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;

public final class CacheDirectives
{

    public static final String NO_CACHE = "no-cache";
    public static final String S_MAXAGE = "s-maxage";
    public static final String MAX_AGE = "max-age";
    public static final String PUBLIC = "public";
    public static final String PRIVATE = "private";
    public static final String NO_STORE = "no-store";
    public static final String ONLY_IF_CACHED = "only-if-cached";
    public static final String MAX_STALE = "max-stale";
    public static final String MIN_FRESH = "min-fresh";

    private CacheDirectives()
    {
        // Utility class
    }

    public static final Predicate<? super HttpHeaderFW> IS_ONLY_IF_CACHED = h ->
    {
        String name = h.name().asString();
        String value = h.value().asString();
        return name.equals(HttpHeaders.CACHE_CONTROL) && value.contains(ONLY_IF_CACHED);
    };
    public static final String MAX_AGE_0 = "max-age=0";
}
