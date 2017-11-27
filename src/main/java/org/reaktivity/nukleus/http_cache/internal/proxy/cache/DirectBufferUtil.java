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

import org.agrona.DirectBuffer;

public class DirectBufferUtil
{

    public static final boolean equals(
            DirectBuffer buffer1,
            int offset1,
            int length1,
            DirectBuffer buffer2,
            int offset2,
            int length2)
    {
        boolean result = length1 == length2;
        for (int i = 0; i < length1 && result; i++)
        {
            result = buffer1.getByte(offset1 + i) == buffer2.getByte(offset2 + i);
        }
        return result;
    }
}
