/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.metadata;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.security.CurrentUser;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Test graph builder.
 */
public class GraphBuilderTest {

    public static final String FALCON_USER = "falcon-user";
    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String PROCESS_ENTITY_NAME = "sample-process";
    public static final String COLO_NAME = "west-coast";
    public static final String WORKFLOW_NAME = "imp-click-join-workflow";
    public static final String WORKFLOW_VERSION = "2.0.9";

    public static final String INPUT_FEED_NAMES = "impression-feed,clicks-feed";
    public static final String OUTPUT_FEED_NAMES = "imp-click-join1,imp-click-join2";

    private ConfigurationStore store;
    private GraphBuilder graphBuilder;

    @BeforeMethod
    public void setUp() throws Exception {
        CurrentUser.authenticate(FALCON_USER);

        store = ConfigurationStore.get();
        graphBuilder = new GraphBuilder();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        cleanupGraphStore();
        cleanupConfigurationStore();
    }

    private void cleanupGraphStore() {
        KeyIndexableGraph graph = graphBuilder.getGraph();
        for (Edge edge : graph.getEdges()) {
            graph.removeEdge(edge);
        }

        for (Vertex vertex : graph.getVertices()) {
            graph.removeVertex(vertex);
        }

        graph.shutdown();
    }

    private void cleanupConfigurationStore() throws FalconException {
        store = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }

    @Test
    public void testOnAdd() throws Exception {
        Cluster clusterEntity = buildCluster(CLUSTER_ENTITY_NAME, COLO_NAME, "classification=production");
        store.publish(EntityType.CLUSTER, clusterEntity);
        verifyEntityWasAddedToGraph(CLUSTER_ENTITY_NAME, GraphBuilder.CLUSTER_ENTITY_TYPE);
        verifyClusterEntityEdges();

        Feed impressionsFeed = buildFeed("impression-feed", clusterEntity,
                "classified-as=Secure", "analytics");
        store.publish(EntityType.FEED, impressionsFeed);
        verifyEntityWasAddedToGraph(impressionsFeed.getName(), GraphBuilder.FEED_ENTITY_TYPE);
        verifyFeedEntityEdges(impressionsFeed.getName());

        Feed clicksFeed = buildFeed("clicks-feed", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "analytics");
        store.publish(EntityType.FEED, clicksFeed);
        verifyEntityWasAddedToGraph(clicksFeed.getName(), GraphBuilder.FEED_ENTITY_TYPE);

        Feed join1Feed = buildFeed("imp-click-join1", clusterEntity,
                "classified-as=Financial", "reporting,bi");
        store.publish(EntityType.FEED, join1Feed);
        verifyEntityWasAddedToGraph(join1Feed.getName(), GraphBuilder.FEED_ENTITY_TYPE);

        Feed join2Feed = buildFeed("imp-click-join2", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "reporting,bi");
        store.publish(EntityType.FEED, join2Feed);
        verifyEntityWasAddedToGraph(join2Feed.getName(), GraphBuilder.FEED_ENTITY_TYPE);

        Process process = buildProcess(PROCESS_ENTITY_NAME, clusterEntity, "classified-as=Critical");
        addWorkflow(process);

        addInput(process, impressionsFeed);
        addInput(process, clicksFeed);

        addOutput(process, join1Feed);
        addOutput(process, join2Feed);

        store.publish(EntityType.PROCESS, process);
        verifyEntityWasAddedToGraph(process.getName(), GraphBuilder.PROCESS_ENTITY_TYPE);
        verifyProcessEntityEdges();

        graphBuilder.debug();

        verifyQuery();
    }

    private static Cluster buildCluster(String name, String colo, String tags) {
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setColo(colo);
        cluster.setTags(tags);

        return cluster;
    }

    private static Feed buildFeed(String feedName, Cluster cluster, String tags, String groups) {
        Feed feed = new Feed();
        feed.setName(feedName);
        feed.setTags(tags);
        feed.setGroups(groups);

        org.apache.falcon.entity.v0.feed.Clusters
                clusters = new org.apache.falcon.entity.v0.feed.Clusters();
        feed.setClusters(clusters);
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
        feedCluster.setName(cluster.getName());
        clusters.getClusters().add(feedCluster);

        return feed;
    }

    private Process buildProcess(String processName, Cluster cluster,
                                 String tags) throws FalconException {
        Process processEntity = new Process();
        processEntity.setName(processName);
        processEntity.setTags(tags);

        org.apache.falcon.entity.v0.process.Cluster processCluster =
                new org.apache.falcon.entity.v0.process.Cluster();
        processCluster.setName(cluster.getName());
        processEntity.setClusters(new org.apache.falcon.entity.v0.process.Clusters());
        processEntity.getClusters().getClusters().add(processCluster);

        return processEntity;
    }

    private void addWorkflow(Process process) {
        Workflow workflow = new Workflow();
        workflow.setName(WORKFLOW_NAME);
        workflow.setVersion(WORKFLOW_VERSION);
        workflow.setEngine(EngineType.PIG);
        workflow.setPath("/falcon/test/workflow");

        process.setWorkflow(workflow);
    }

    private void addInput(Process process, Feed feed) {
        if (process.getInputs() == null) {
            process.setInputs(new Inputs());
        }

        Inputs inputs = process.getInputs();
        Input input = new Input();
        input.setFeed(feed.getName());
        inputs.getInputs().add(input);
    }

    private void addOutput(Process process, Feed feed) {
        if (process.getOutputs() == null) {
            process.setOutputs(new Outputs());
        }

        Outputs outputs = process.getOutputs();
        Output output = new Output();
        output.setFeed(feed.getName());
        outputs.getOutputs().add(output);
    }

    private void verifyEntityWasAddedToGraph(String entityName, String entityType) {
        Vertex entityVertex = getEntityVertex(entityName, entityType);
        Assert.assertNotNull(entityVertex);
        verifyEntityProperties(entityVertex, entityName, entityType);
    }

    private void verifyEntityProperties(Vertex entityVertex, String entityName, String entityType) {
        Assert.assertEquals(entityName, entityVertex.getProperty(GraphBuilder.NAME_PROPERTY_KEY));
        Assert.assertEquals(entityType, entityVertex.getProperty(GraphBuilder.TYPE_PROPERTY_KEY));
        Assert.assertNotNull(entityVertex.getProperty(GraphBuilder.TIMESTAMP_PROPERTY_KEY));
    }

    private void verifyClusterEntityEdges() {
        Vertex clusterVertex = getEntityVertex(CLUSTER_ENTITY_NAME, GraphBuilder.CLUSTER_ENTITY_TYPE);

        // verify edge to user vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, GraphBuilder.USER_LABEL,
                FALCON_USER, GraphBuilder.USER_TYPE);
        // verify edge to colo vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, GraphBuilder.CLUSTER_COLO_LABEL,
                COLO_NAME, GraphBuilder.COLO_TYPE);
        // verify edge to tags vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, "classification",
                "production", GraphBuilder.TAGS_TYPE);
    }

    private void verifyFeedEntityEdges(String feedName) {
        Vertex feedVertex = getEntityVertex(feedName, GraphBuilder.FEED_ENTITY_TYPE);

        // verify edge to cluster vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, GraphBuilder.FEED_CLUSTER_EDGE_LABEL,
                CLUSTER_ENTITY_NAME, GraphBuilder.CLUSTER_ENTITY_TYPE);
        // verify edge to user vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, GraphBuilder.USER_LABEL,
                FALCON_USER, GraphBuilder.USER_TYPE);
        // verify edge to tags vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, "classified-as",
                "Secure", GraphBuilder.TAGS_TYPE);
        // verify edge to group vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, GraphBuilder.GROUPS_LABEL,
                "analytics", GraphBuilder.GROUPS_TYPE);
    }

    private void verifyProcessEntityEdges() {
        Vertex processVertex = getEntityVertex(PROCESS_ENTITY_NAME, GraphBuilder.PROCESS_ENTITY_TYPE);

        // verify edge to cluster vertex
        verifyVertexForEdge(processVertex, Direction.OUT, GraphBuilder.FEED_CLUSTER_EDGE_LABEL,
                CLUSTER_ENTITY_NAME, GraphBuilder.CLUSTER_ENTITY_TYPE);
        // verify edge to user vertex
        verifyVertexForEdge(processVertex, Direction.OUT, GraphBuilder.USER_LABEL,
                FALCON_USER, GraphBuilder.USER_TYPE);
        // verify edge to tags vertex
        verifyVertexForEdge(processVertex, Direction.OUT, "classified-as",
                "Critical", GraphBuilder.TAGS_TYPE);

        // verify edge to inputs vertex
        for (Edge edge : processVertex.getEdges(Direction.OUT, GraphBuilder.FEED_PROCESS_EDGE_LABEL)) {
            Vertex outVertex = edge.getVertex(Direction.OUT);
            Assert.assertEquals(GraphBuilder.FEED_ENTITY_TYPE,
                    outVertex.getProperty(GraphBuilder.TYPE_PROPERTY_KEY));
            String name = outVertex.getProperty(GraphBuilder.NAME_PROPERTY_KEY);
            if (!(name.equals("impression-feed") || name.equals("clicks-feed"))) {
                Assert.fail("feed name should have been one of impression-feed or clicks-feed");
            }
        }

        // verify edge to outputs vertex
        for (Edge edge : processVertex.getEdges(Direction.IN, GraphBuilder.PROCESS_FEED_EDGE_LABEL)) {
            Vertex outVertex = edge.getVertex(Direction.IN);
            Assert.assertEquals(GraphBuilder.FEED_ENTITY_TYPE,
                    outVertex.getProperty(GraphBuilder.TYPE_PROPERTY_KEY));
            String name = outVertex.getProperty(GraphBuilder.NAME_PROPERTY_KEY);
            if (!(name.equals("imp-click-join1") || name.equals("imp-click-join2"))) {
                Assert.fail("feed name should have been one of imp-click-join1 or imp-click-join2");
            }
        }
    }

    private Vertex getEntityVertex(String entityName, String entityType) {
        GraphQuery entityQuery = graphBuilder.getQuery()
                .has(GraphBuilder.NAME_PROPERTY_KEY, entityName)
                .has(GraphBuilder.TYPE_PROPERTY_KEY, entityType);
        Iterator<Vertex> iterator = entityQuery.vertices().iterator();
        Assert.assertTrue(iterator.hasNext());

        Vertex entityVertex = iterator.next();
        Assert.assertNotNull(entityVertex);

        return entityVertex;
    }

    private void verifyVertexForEdge(Vertex vertex, Direction direction, String label,
                                     String expectedName, String expectedType) {
        for (Edge edge : vertex.getEdges(direction, label)) {
            Vertex outVertex = edge.getVertex(Direction.IN);
            Assert.assertEquals(outVertex.getProperty(GraphBuilder.NAME_PROPERTY_KEY), expectedName);
            Assert.assertEquals(outVertex.getProperty(GraphBuilder.TYPE_PROPERTY_KEY), expectedType);
        }
    }

    private void verifyQuery() {
        System.out.println();
        System.out.println();

        // feeds owned by a user
        GraphQuery userQuery = graphBuilder.getQuery()
                .has(GraphBuilder.NAME_PROPERTY_KEY, FALCON_USER)
                .has(GraphBuilder.TYPE_PROPERTY_KEY, GraphBuilder.USER_TYPE);

        for (Vertex userVertex : userQuery.vertices()) {
            for (Vertex feed : userVertex.getVertices(Direction.IN, GraphBuilder.USER_LABEL)) {
                if (feed.getProperty(GraphBuilder.TYPE_PROPERTY_KEY).equals( GraphBuilder.FEED_ENTITY_TYPE)) {
                    System.out.println(FALCON_USER + " owns -> " + GraphBuilder.vertexString(feed));
                }
            }
        }

        System.out.println("--------------------------------------");
        // feeds classified as secure
        GraphQuery classQuery = graphBuilder.getQuery()
                .has(GraphBuilder.NAME_PROPERTY_KEY, "Secure")
                .has(GraphBuilder.TYPE_PROPERTY_KEY, GraphBuilder.TAGS_TYPE);

        for (Vertex feedVertex : classQuery.vertices()) {
            for (Vertex feed : feedVertex.getVertices(Direction.BOTH, "classified-as")) {
                if (feed.getProperty(GraphBuilder.TYPE_PROPERTY_KEY).equals( GraphBuilder.FEED_ENTITY_TYPE)) {
                    System.out.println(" Secure classification -> " + GraphBuilder.vertexString(feed));
                }
            }
        }

        System.out.println("--------------------------------------");
        // feeds owned by a user and classified as secure
        Vertex userVertex = getEntityVertex(FALCON_USER, GraphBuilder.USER_TYPE);
        for (Vertex feed : userVertex.getVertices(Direction.IN, GraphBuilder.USER_LABEL)) {
            for (Vertex classVertex : feed.getVertices(Direction.OUT, "classified-as")) {
                if (classVertex.getProperty(GraphBuilder.NAME_PROPERTY_KEY).equals("Secure")) {
                    System.out.println(" Secure feed owned by falcon-user -> "
                            + GraphBuilder.vertexString(feed));
                }
            }
        }
    }

    @Test (enabled = false)
    public void testAddLineageToGraph() throws Exception {
        Map<String, String> lineage = getTestData();
        graphBuilder.addLineageToGraph(lineage);
        graphBuilder.debug();
    }

    private static Map<String, String> getTestData() {
        Map<String, String> lineage = new HashMap<String, String>();
        lineage.put("nominalTime", "2014-01-01-01-00");
        lineage.put("timeStamp", "2014-01-01-01-00");

        lineage.put("entityName", PROCESS_ENTITY_NAME);
        lineage.put("entityType", "process");
        lineage.put("cluster", CLUSTER_ENTITY_NAME);
        lineage.put("operation", "GENERATE");

        lineage.put("workflowUser", "falcon-user");
        lineage.put("workflowEngineUrl", "http://localhost:11000/oozie");
        lineage.put("subflowId", "userflow@wf-id");
        lineage.put("userWorkflowEngine", "oozie");
        lineage.put("workflowId", "workflow-01-00");
        lineage.put("runId", "1");
        lineage.put("status", "SUCCEEDED");

        lineage.put("falconInputFeeds",  INPUT_FEED_NAMES);
        lineage.put("falconInPaths",
                "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20");

        lineage.put("feedNames",  OUTPUT_FEED_NAMES);
        lineage.put("feedInstancePaths",
                "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20");

        lineage.put("logDir", "target/log");
        return lineage;
    }
}
