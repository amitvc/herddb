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

package herddb.cluster;

import io.netty.util.HashedWheelTimer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookiesHealthInfo;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.proto.LocalBookiesRegistry;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Copied from DefaultEnsemblePlacementPolicy
 *
 * @author francesco.caliumi
 */
public class PreferLocalBookiePlacementPolicy implements EnsemblePlacementPolicy {

    private Set<BookieId> knownBookies = new HashSet<>();

    @Override
    public EnsemblePlacementPolicy initialize(
            ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver,
            HashedWheelTimer hashedWheelTimer,
            FeatureProvider featureProvider, StatsLogger statsLogger,
            BookieAddressResolver bookieAddressResolver
    ) {
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    @Override
    public synchronized Set<BookieId> onClusterChanged(
            Set<BookieId> writableBookies,
            Set<BookieId> readOnlyBookies
    ) {
        HashSet<BookieId> deadBookies;
        deadBookies = new HashSet<>(knownBookies);
        deadBookies.removeAll(writableBookies);
        // readonly bookies should not be treated as dead bookies
        deadBookies.removeAll(readOnlyBookies);
        knownBookies = writableBookies;
        return deadBookies;
    }

    @Override
    public PlacementResult<BookieId> replaceBookie(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Map<String, byte[]> customMetadata,
            List<BookieId> currentEnsemble,
            BookieId bookieToReplace,
            Set<BookieId> excludeBookies
    ) throws BKNotEnoughBookiesException {
        excludeBookies.addAll(currentEnsemble);
        PlacementResult<List<BookieId>> list = newEnsemble(1, 1, 1, customMetadata, excludeBookies);
        return PlacementResult.of(list.getResult().get(0), PlacementPolicyAdherence.MEETS_STRICT);
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(List<BookieId> ensemble, BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(List<BookieId> ensemble, BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public void registerSlowBookie(BookieId bookieSocketAddress, long entryId) {
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Map<String, byte[]> customMetadata,
            Set<BookieId> excludeBookies
    )
            throws BKNotEnoughBookiesException {

        ArrayList<BookieId> newBookies = new ArrayList<>(ensembleSize);
        if (ensembleSize <= 0) {
            return PlacementResult.of(newBookies, PlacementPolicyAdherence.MEETS_STRICT);
        }
        List<BookieId> allBookies;
        synchronized (this) {
            allBookies = new ArrayList<>(knownBookies);
        }

        BookieId localBookie = null;
        for (BookieId bookie : allBookies) {
            if (excludeBookies.contains(bookie)) {
                continue;
            }
            if (LocalBookiesRegistry.isLocalBookie(bookie)) {
                localBookie = bookie;
                break;
            }
        }
        if (localBookie != null) {
            boolean ret = allBookies.remove(localBookie);
            if (!ret) {
                throw new RuntimeException("localBookie not found in list");
            }
            newBookies.add(localBookie);
            --ensembleSize;
            if (ensembleSize == 0) {
                return PlacementResult.of(newBookies, PlacementPolicyAdherence.MEETS_STRICT);
            }
        }

        Collections.shuffle(allBookies);
        for (BookieId bookie : allBookies) {
            if (excludeBookies.contains(bookie)) {
                continue;
            }
            newBookies.add(bookie);
            --ensembleSize;
            if (ensembleSize == 0) {
                return PlacementResult.of(newBookies, PlacementPolicyAdherence.MEETS_STRICT);
            }
        }

        throw new BKException.BKNotEnoughBookiesException();
    }

    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieId> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        return PlacementPolicyAdherence.MEETS_STRICT;
    }

}
