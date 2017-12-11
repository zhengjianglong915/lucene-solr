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
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;

public class ForceLeaderTest extends HttpPartitionTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean onlyLeaderIndexes = random().nextBoolean();

  @Override
  protected boolean useTlogReplicas() {
    return onlyLeaderIndexes;
  }

  /**
   * Test that FORCELEADER can set last published state of all down (live) replicas to active (so
   * that they become worthy candidates for leader election).
   */
  @Slow
  public void testLastPublishedStateIsActive() throws Exception {
    handle.put("maxScore", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    String testCollectionName = "forceleader_last_published";
    createCollection(testCollectionName, "conf1", 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    log.info("Collection created: " + testCollectionName);

    try {
      List<Replica> notLeaders = ensureAllReplicasAreActive(testCollectionName, SHARD1, 1, 3, maxWaitSecsToSeeAllActive);
      assertEquals("Expected 2 replicas for collection " + testCollectionName
          + " but found " + notLeaders.size() + "; clusterState: "
          + printClusterStateInfo(testCollectionName), 2, notLeaders.size());

      Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, SHARD1);
      JettySolrRunner notLeader0 = getJettyOnPort(getReplicaPort(notLeaders.get(0)));
      ZkController zkController = notLeader0.getCoreContainer().getZkController();

      // Mark all replicas down
      setReplicaState(testCollectionName, SHARD1, leader, State.DOWN);
      for (Replica rep : notLeaders) {
        setReplicaState(testCollectionName, SHARD1, rep, State.DOWN);
      }

      zkController.getZkStateReader().forceUpdateCollection(testCollectionName);
      // Assert all replicas are down and that there is no leader
      assertEquals(0, getActiveOrRecoveringReplicas(testCollectionName, SHARD1).size());

      // Now force leader
      doForceLeader(cloudClient, testCollectionName, SHARD1);

      // Assert that last published states of the two replicas are active now
      for (Replica rep: notLeaders) {
        assertEquals(Replica.State.ACTIVE, getLastPublishedState(testCollectionName, SHARD1, rep));
      }
    } finally {
      log.info("Cleaning up after the test.");
      attemptCollectionDelete(cloudClient, testCollectionName);
    }
  }

  protected void setReplicaState(String collection, String slice, Replica replica, Replica.State state) throws Exception {
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(cloudClient.getZkStateReader().getZkClient());
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();

    String baseUrl = zkStateReader.getBaseUrlForNodeName(replica.getNodeName());
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
        ZkStateReader.BASE_URL_PROP, baseUrl,
        ZkStateReader.NODE_NAME_PROP, replica.getNodeName(),
        ZkStateReader.SHARD_ID_PROP, slice,
        ZkStateReader.COLLECTION_PROP, collection,
        ZkStateReader.CORE_NAME_PROP, replica.getStr(CORE_NAME_PROP),
        ZkStateReader.CORE_NODE_NAME_PROP, replica.getName(),
        ZkStateReader.STATE_PROP, state.toString());
    inQueue.offer(Utils.toJSON(m));
    boolean transition = false;

    Replica.State replicaState = null;
    for (int counter = 10; counter > 0; counter--) {
      ClusterState clusterState = zkStateReader.getClusterState();
      replicaState = clusterState.getCollection(collection).getSlice(slice).getReplica(replica.getName()).getState();
      if (replicaState == state) {
        transition = true;
        break;
      }
      Thread.sleep(1000);
    }

    if (!transition) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not set replica [" + replica.getName() + "] as " + state +
          ". Last known state of the replica: " + replicaState);
    }
  }

  protected Replica.State getLastPublishedState(String collection, String slice, Replica replica) throws SolrServerException, IOException,
  KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    String baseUrl = zkStateReader.getBaseUrlForNodeName(replica.getNodeName());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.STATUS.toString());
    params.set(CoreAdminParams.CORE, replica.getStr("core"));

    SolrRequest<SimpleSolrResponse> req = new GenericSolrRequest(METHOD.GET, "/admin/cores", params);
    NamedList resp = null;
    try (HttpSolrClient hsc = getHttpSolrClient(baseUrl)) {
       resp = hsc.request(req);
    }

    String lastPublished = (((NamedList<NamedList<String>>)resp.get("status")).get(replica.getStr("core"))).get("lastPublished");
    return Replica.State.getState(lastPublished);
  }

  void assertSendDocFails(int docId) throws Exception {
    // sending a doc in this state fails
    try {
      sendDoc(docId);
      log.error("Should've failed indexing during a down state. Cluster state: " + printClusterStateInfo());
      fail("Should've failed indexing during a down state.");
    } catch (SolrException ex) {
      log.info("Document couldn't be sent, which is expected.");
    }
  }

  @Override
  protected int sendDoc(int docId) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);

    return sendDocsWithRetry(Collections.singletonList(doc), 1, 5, 1);
  }

  private void doForceLeader(SolrClient client, String collectionName, String shard) throws IOException, SolrServerException {
    CollectionAdminRequest.ForceLeader forceLeader = CollectionAdminRequest.forceLeaderElection(collectionName, shard);
    client.request(forceLeader);
  }

}

