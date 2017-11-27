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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

public class DirectBufferUtilTest
{

    @Test
    public void shouldCompareBuffers()
    {
        final DirectBuffer buffer1 = new UnsafeBuffer("abcdef".getBytes(UTF_8));
        final DirectBuffer buffer2 = new UnsafeBuffer("abcdefghi".getBytes(UTF_8));
        Assert.assertTrue(DirectBufferUtil.equals(buffer1, 0, 6, buffer2, 0, 6));
        Assert.assertTrue(DirectBufferUtil.equals(buffer1, 2, 4, buffer2, 2, 4));
        Assert.assertFalse(DirectBufferUtil.equals(buffer1, 0, 6, buffer2, 1, 6));
        Assert.assertFalse(DirectBufferUtil.equals(buffer1, 0, 6, buffer2, 1, 7));
        Assert.assertFalse(DirectBufferUtil.equals(buffer1, 0, 6, buffer2, 0, 5));
    }

}
