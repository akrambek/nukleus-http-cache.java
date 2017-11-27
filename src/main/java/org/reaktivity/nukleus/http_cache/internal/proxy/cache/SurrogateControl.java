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

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.SURROGATE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class SurrogateControl
{
    public static final String MAX_AGE = "max-age";
    private static final Pattern CACHE_PATTERN = Pattern
            .compile("\\s*([\\w\\-]+)\\s*(=)?\\s*(\\d+\\+?\\d+|\\\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)+\\\")?\\s*");
    private static final String X_PROTECTED = "x-protected";

    public static int getSurrogateFreshnessExtension(
            ListFW<HttpHeaderFW> responseHeadersRO)
    {
        String surrogateControl = getHeader(responseHeadersRO, SURROGATE_CONTROL);
        return getSurrogateFreshnessExtension(surrogateControl);
    }

    public static int getSurrogateFreshnessExtension(String headerValue)
    {
        if (headerValue != null)
        {
            Matcher matcher = CACHE_PATTERN.matcher(headerValue);
            while (matcher.find())
            {
                if (MAX_AGE.equals(matcher.group(1)))
                {
                    String maxAge = matcher.group(3);
                    if (maxAge.contains("+"))
                    {
                        // TODO change to matcher
                        final String value = maxAge.split("\\+")[1];
                        return Integer.parseInt(value);
                    }
                    else
                    {
                        return -1;
                    }
                }
            }
        }
        return -1;
    }

    public static int getSurrogateAge(
            ListFW<HttpHeaderFW> responseHeadersRO)
    {
        String surrogateControl = getHeader(responseHeadersRO, SURROGATE_CONTROL);
        return getSurrogateAge(surrogateControl);
    }

    public static int getSurrogateAge(String headerValue)
    {
        if (headerValue != null)
        {
            Matcher matcher = CACHE_PATTERN.matcher(headerValue);
            while (matcher.find())
            {
                if (MAX_AGE.equals(matcher.group(1)))
                {
                    String maxAge = matcher.group(3);
                    if (maxAge.contains("+"))
                    {
                        // TODO change to matcher
                        final String value = maxAge.split("\\+")[0];
                        return Integer.parseInt(value);
                    }
                    else
                    {
                        return -1;
                    }
                }
            }
        }
        return -1;
    }

    public static boolean isProtectedEx(ListFW<HttpHeaderFW> response)
    {
        String surrogateControl = getHeader(response, SURROGATE_CONTROL);
        if (surrogateControl != null)
        {
            Matcher matcher = CACHE_PATTERN.matcher(surrogateControl);
            while (matcher.find())
            {
                if (X_PROTECTED.equals(matcher.group(1)))
                {
                    return true;
                }
            }
        }
        return false;
    }

}
