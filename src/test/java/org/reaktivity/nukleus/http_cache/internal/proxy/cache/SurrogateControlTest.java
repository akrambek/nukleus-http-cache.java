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

import org.junit.Assert;
import org.junit.Test;

public class SurrogateControlTest
{

    @Test
    public void shouldParseSurrogateMaxAge()
    {
        Assert.assertEquals(1, SurrogateControl.getSurrogateAge("max-age=1+2"));
        Assert.assertEquals(30, SurrogateControl.getSurrogateAge("max-age=30+2147483647, x-protected"));
        Assert.assertEquals(2, SurrogateControl.getSurrogateFreshnessExtension("max-age=1+2"));
        Assert.assertEquals(2147483647, SurrogateControl.getSurrogateFreshnessExtension("max-age=30+2147483647, x-protected"));
    }
}
