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

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableList;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_AGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.NO_CACHE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.PUBLIC;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.S_MAXAGE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.METHOD;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.TRANSFER_ENCODING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class CacheUtils
{

    public static final List<String> CACHEABLE_BY_DEFAULT_STATUS_CODES = unmodifiableList(
            asList("200", "203", "204", "206", "300", "301", "404", "405", "410", "414", "501"));

    public static final String LAST_MODIFIED = "last-modified";

    private CacheUtils()
    {
        // utility class
    }

    public static boolean canBeServedByCache(
        ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
                case CACHE_CONTROL:
                    // TODO remove need for max-age=0 (Currently can't handle multiple outstanding cache updates)
                    return value.contains(CacheDirectives.NO_CACHE) || value.contains(CacheDirectives.MAX_AGE_0);
                case METHOD:
                    return !HttpMethods.GET.equalsIgnoreCase(value);
                case CONTENT_LENGTH:
                    return true;
                case TRANSFER_ENCODING:
                    return true;
                default:
                    return false;
                }
        });
    }

    public static boolean isCacheableResponse(ListFW<HttpHeaderFW> response)
    {
        if (response.anyMatch(h ->
                CACHE_CONTROL.equals(h.name().asString())
                && h.value().asString().contains(CacheDirectives.PRIVATE)))
        {
            return false;
        }
        return isPrivatelyCacheable(response);
    }

    public static boolean isPrivatelyCacheable(ListFW<HttpHeaderFW> response)
    {
        // TODO force passing of CacheControl as FW
        String cacheControl = getHeader(response, HttpHeaders.CACHE_CONTROL);
        if (cacheControl != null)
        {
            CacheControl parser = new CacheControl().parse(cacheControl);
            Iterator<String> iter = parser.iterator();
            while(iter.hasNext())
            {
                String directive = iter.next();
                switch(directive)
                {
                    // TODO expires
                    case NO_CACHE:
                        return false;
                    case PUBLIC:
                        return true;
                    case MAX_AGE:
                        return true;
                    case S_MAXAGE:
                        return true;
                    default:
                        break;
                }
            }
        }
        return response.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            if (STATUS.equals(name))
            {
                return CACHEABLE_BY_DEFAULT_STATUS_CODES.contains(value);
            }
            return false;
        });
    }

    public static boolean sameAuthorizationScope(
        ListFW<HttpHeaderFW> request,
        ListFW<HttpHeaderFW> cachedRequest,
        CacheControl cachedResponse)
    {
        if (cachedResponse.contains(CacheDirectives.PUBLIC))
        {
            return true;
        }

        if (cachedResponse.contains(CacheDirectives.PRIVATE))
        {
            return false;
        }

        final String cachedAuthorizationHeader = getHeader(cachedRequest, "authorization");
        final String requestAuthorizationHeader = getHeader(request, "authorization");
        if (cachedAuthorizationHeader != null || requestAuthorizationHeader != null)
        {
            return false;
        }
        return true;
    }

    public static boolean doesNotVary(
        ListFW<HttpHeaderFW> request,
        ListFW<HttpHeaderFW> cachedResponse,
        ListFW<HttpHeaderFW> cachedRequest)
    {
        final String cachedVaryHeader = getHeader(cachedResponse, "vary");
        if (cachedVaryHeader == null)
        {
            return true;
        }

        return stream(cachedVaryHeader.split("\\s*,\\s*")).noneMatch(v ->
        {
            String pendingHeaderValue = getHeader(request, v);
            String myHeaderValue = getHeader(cachedRequest, v);
            return !Objects.equals(pendingHeaderValue, myHeaderValue);
        });
    }

    public static boolean isMatchByEtag(
        ListFW<HttpHeaderFW> requestHeaders,
        String etag)
    {
        String ifMatch = HttpHeadersUtil.getHeader(requestHeaders, HttpHeaders.IF_NONE_MATCH);
        if (ifMatch == null)
        {
            return false;
        }

        // TODO, use Java Pattern for less GC
        return Arrays.stream(ifMatch.split(",")).anyMatch(t -> etag.equals(t.trim()));
    }

}
