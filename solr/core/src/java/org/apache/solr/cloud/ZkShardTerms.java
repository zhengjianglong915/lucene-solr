/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.impl.ZkDistribStateManager;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkShardTerms implements AutoCloseable{

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Object writingLock = new Object();
  private final AtomicInteger numWatcher = new AtomicInteger(0);
  private final String collection;
  private final String shard;
  private final String znodePath;
  private final DistribStateManager stateManager;
  private final Set<CoreTermWatcher> listeners = new HashSet<>();

  private Map<String, Long> terms = new HashMap<>();
  private int version = 0;

  interface CoreTermWatcher {
    // return true if the listener wanna to be triggered in the next time
    boolean onTermChanged(ZkShardTerms zkShardTerms);
  }

  public ZkShardTerms(String collection, String shard, SolrZkClient client) {
    this.znodePath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms/" + shard;
    this.collection = collection;
    this.shard = shard;
    this.stateManager = new ZkDistribStateManager(client);
    ensureTermNodeExist();
    updateTerms();
    ObjectReleaseTracker.track(this);
  }

  public boolean ensureTermsIsHigher(String leader, Set<String> replicasInLowerTerms) {
    while(!isLessThanLeaderTerm(leader, replicasInLowerTerms)) {
      synchronized (writingLock) {
        long leaderTerm = terms.get(leader);
        for (String replica : terms.keySet()) {
          if (Objects.equals(terms.get(replica), leaderTerm) && !replicasInLowerTerms.contains(replica)) {
            terms.put(replica, leaderTerm+1);
          }
        }
        if (forceSaveTerms()) return true;
      }
    }
    return false;
  }

  public boolean canBecomeLeader(String coreNodeName) {
    if (terms.isEmpty()) return true;
    long maxTerm = Collections.max(terms.values());
    return terms.getOrDefault(coreNodeName, 0L) == maxTerm;
  }

  public void close() {
    // no watcher will be registered
    numWatcher.addAndGet(1);
    ObjectReleaseTracker.release(this);
  }

  // package private for testing, only used by tests
  HashMap<String, Long> getTerms() {
    return new HashMap<>(terms);
  }

  void addListener(CoreTermWatcher listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  void removeTerm(String collection, CoreDescriptor cd) {
    synchronized (listeners) {
      // solrcore already closed
      listeners.removeIf(coreTermWatcher -> !coreTermWatcher.onTermChanged(this));
    }
    while (true) {
      synchronized (writingLock) {
        terms.remove(cd.getCloudDescriptor().getCoreNodeName());
        try {
          if (saveTerms()) break;
        } catch (NoSuchElementException e) {
          return;
        }
      }
    }
  }

  void registerTerm(String replica) {
    while (!terms.containsKey(replica)) {
      synchronized (writingLock) {
        terms.put(replica, 0L);
        forceSaveTerms();
      }
    }
  }

  void setEqualsToMax(String replica) {
    while (true){
      synchronized (writingLock) {
        long maxTerm;
        try {
          maxTerm = Collections.max(terms.values());
        } catch (NoSuchElementException e){
          maxTerm = 0;
        }
        terms.put(replica, maxTerm);
        if (forceSaveTerms()) break;
      }
    }
  }

  int getNumListeners() {
    synchronized (listeners) {
      return listeners.size();
    }
  }

  private boolean forceSaveTerms() {
    try {
      return saveTerms();
    } catch (NoSuchElementException e) {
      ensureTermNodeExist();
      return false;
    }
  }

  private boolean saveTerms() throws NoSuchElementException {
    byte[] znodeData = Utils.toJSON(terms);
    // must retry on conn loss otherwise future election attempts may assume wrong LIR state
    try {
      stateManager.setData(znodePath, znodeData, version);
      return true;
    } catch (BadVersionException e) {
      updateTerms();
    } catch (NoSuchElementException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }


  private void ensureTermNodeExist() {
    String path = "/collections/"+collection+ "/terms";
    try {
      if (!stateManager.hasData(path)) {
        try {
          stateManager.makePath(path);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
      path += "/"+shard;
      if (!stateManager.hasData(path)) {
        try {
          Map<String, Long> initialTerms = new HashMap<>();
          stateManager.createData(path, Utils.toJSON(initialTerms), CreateMode.PERSISTENT);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
    }  catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating shard term node in Zookeeper for collection:" + collection, e);
    } catch (IOException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating shard term node in Zookeeper for collection:" + collection, e);
    }
  }

  private void updateTerms() {
    try {
      Watcher watcher = null;
      if (numWatcher.compareAndSet(0, 1)) {
        watcher = event -> {
          numWatcher.decrementAndGet();
          updateTerms();
        };
      }

      VersionedData data = stateManager.getData(znodePath, watcher);
      version = data.getVersion();
      terms = (Map<String, Long>) Utils.fromJSON(data.getData());
      onTermUpdates();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private boolean isLessThanLeaderTerm(String leader, Set<String> replicasInLowerTerms) {
    for (String replica : replicasInLowerTerms) {
      if (!terms.containsKey(leader)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can not find leader's term " + leader);
      }
      if (!terms.containsKey(replica)) return false;
      if (terms.get(leader) <= terms.get(replica)) return false;
    }
    return true;
  }

  private void onTermUpdates() {
    synchronized (listeners) {
      listeners.removeIf(coreTermWatcher -> !coreTermWatcher.onTermChanged(this));
    }
  }
}
