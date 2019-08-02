/**
 * Copyright 2016-2019 The Reaktivity Project
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

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;

import java.util.List;
import java.util.function.LongConsumer;

public class Int2CacheHashMapWithLRUEviction
{

    private static final int PURGE_SIZE = 1;
    private final Int2ObjectHashMap<DefaultCacheEntry> cachedEntries;
    private final IntArrayList lruEntryList;
    private final LongConsumer entryCount;

    public Int2CacheHashMapWithLRUEviction(LongConsumer entryCount)
    {
        cachedEntries = new Int2ObjectHashMap<>();
        lruEntryList = new IntArrayList();
        this.entryCount = entryCount;
    }

    public void put(
        int requestUrlHash,
        DefaultCacheEntry cacheEntry)
    {
        DefaultCacheEntry old = cachedEntries.put(requestUrlHash, cacheEntry);
        if (old == null)
        {
            entryCount.accept(1);
        }
        lruEntryList.removeInt(requestUrlHash);
        lruEntryList.add(requestUrlHash);

        assert cachedEntries.size() == lruEntryList.size();
    }

    public DefaultCacheEntry get(int requestUrlHash)
    {
        final DefaultCacheEntry result = cachedEntries.get(requestUrlHash);
        if (result != null)
        {
            lruEntryList.removeInt(requestUrlHash);
            lruEntryList.add(requestUrlHash);
        }
        return result;
    }

    public DefaultCacheEntry remove(int requestUrlHash)
    {
        final DefaultCacheEntry result = cachedEntries.remove(requestUrlHash);
        if (result != null)
        {
            lruEntryList.removeInt(requestUrlHash);
            entryCount.accept(-1);
        }

        assert cachedEntries.size() == lruEntryList.size();

        return result;
    }

    public Int2ObjectHashMap<DefaultCacheEntry> getCachedEntries()
    {
        return cachedEntries;
    }

    /*
     * @return true if entries are purged
     *         false otherwise
     */
    public boolean purgeLRU()
    {
        if (lruEntryList.size() < PURGE_SIZE)
        {
            return false;
        }

        final List<Integer> subList = lruEntryList.subList(0, PURGE_SIZE);
        subList.forEach(i ->
        {
            DefaultCacheEntry rm = cachedEntries.remove(i);
            assert rm != null;
            rm.purge();
        });
        entryCount.accept(-subList.size());
        subList.clear();

        assert cachedEntries.size() == lruEntryList.size();
        return true;
    }
}
