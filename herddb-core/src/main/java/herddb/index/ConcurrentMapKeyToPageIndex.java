/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.index;

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of KeyToPageIndex which uses any ConcurrentMap
 *
 * @author enrico.olivelli
 */
public class ConcurrentMapKeyToPageIndex implements KeyToPageIndex {

    private final ConcurrentMap<Bytes, Long> map;

    public ConcurrentMapKeyToPageIndex(ConcurrentMap<Bytes, Long> map) {
        this.map = map;
    }

    public ConcurrentMap<Bytes, Long> getMap() {
        return map;
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public Long put(Bytes key, long currentPage) {
        return map.put(key, currentPage);
    }

    @Override
    public List<Bytes> getKeysMappedToPage(long pageId) {
        return map.entrySet().stream().filter(entry -> pageId == entry.getValue()).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    @Override
    public boolean containsKey(Bytes key) {
        return map.containsKey(key);
    }

    @Override
    public Long get(Bytes key) {
        return map.get(key);
    }

    @Override
    public Long remove(Bytes key) {
        return map.remove(key);
    }

    @Override
    public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext, herddb.core.AbstractIndexManager index) throws DataStorageManagerException {

        if (operation instanceof PrimaryIndexSeek) {
            try {
                PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
                byte[] seekValue = seek.value.computeNewValue(null, context, tableContext);
                if (seekValue == null) {
                    return Stream.empty();
                }
                Bytes key = Bytes.from_array(seekValue);
                Long pageId = map.get(key);
                if (pageId == null) {
                    return Stream.empty();
                }
                return Stream.of(new AbstractMap.SimpleImmutableEntry<Bytes, Long>(key, pageId));
            } catch (StatementExecutionException err) {
                throw new DataStorageManagerException(err);
            }
        }

        // Remember that the IndexOperation can return more records
        // every predicate (WHEREs...) will always be evaluated anyway on every record, in order to guarantee correctness        
        if (index != null) {
            try {
                return index.recordSetScanner(operation, context, tableContext, this);
            } catch (StatementExecutionException err) {
                throw new DataStorageManagerException(err);
            }
        }
        if (operation == null) {
            Stream<Map.Entry<Bytes, Long>> baseStream = map.entrySet().stream();
            return baseStream;
        } else if (operation instanceof PrimaryIndexPrefixScan) {
            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
            byte[] prefix;
            try {
                prefix = scan.value.computeNewValue(null, context, tableContext);
            } catch (StatementExecutionException err) {
                throw new RuntimeException(err);
            }
            Predicate<Map.Entry<Bytes, Long>> predicate = (Map.Entry<Bytes, Long> t) -> {
                byte[] fullrecordKey = t.getKey().data;
                return Bytes.startsWith(fullrecordKey, prefix.length, prefix);
            };
            Stream<Map.Entry<Bytes, Long>> baseStream = map.entrySet().stream();
            return baseStream.filter(predicate);
        } else {
            throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());
        }
    }

    @Override
    public void close() {
        map.clear();
    }

    @Override
    public void truncate() {
        map.clear();
    }

}
