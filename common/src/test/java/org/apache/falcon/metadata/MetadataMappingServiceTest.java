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
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.security.CurrentUser;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Test for Metadata relationship mapping service.
 */
public class MetadataMappingServiceTest {

    public static final String FALCON_USER = "falcon-user";
    private static final String LOGS_DIR = "target/log";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";

    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String PROCESS_ENTITY_NAME = "sample-process";
    public static final String COLO_NAME = "west-coast";
    public static final String WORKFLOW_NAME = "imp-click-join-workflow";
    public static final String WORKFLOW_VERSION = "1.0.9";

    public static final String INPUT_FEED_NAMES = "impression-feed,clicks-feed";
    public static final String INPUT_INSTANCE_PATHS =
        "jail://global:00/falcon/impression-feed/20140101,jail://global:00/falcon/clicks-feed/20140101";

    public static final String OUTPUT_FEED_NAMES = "imp-click-join1,imp-click-join2";
    public static final String OUTPUT_INSTANCE_PATHS =
        "jail://global:00/falcon/imp-click-join1/20140101,jail://global:00/falcon/imp-click-join2/20140101";

    private ConfigurationStore configStore;
    private MetadataMappingService service;

    private Cluster clusterEntity;
    private Cluster bcpCluster;
    private List<Feed> inputFeeds = new ArrayList<Feed>();
    private List<Feed> outputFeeds = new ArrayList<Feed>();
    private Process processEntity;


    @BeforeClass
    public void setUp() throws Exception {
        CurrentUser.authenticate(FALCON_USER);

        configStore = ConfigurationStore.get();

        service = new MetadataMappingService();
        service.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        cleanupGraphStore(service.getGraph());
        cleanupConfigurationStore(configStore);
        service.destroy();
    }

    @AfterMethod
    public void printGraph() {
        service.debug();
    }

    private GraphQuery getQuery() {
        return service.getGraph().query();
    }

    @Test
    public void testGetName() throws Exception {
        Assert.assertEquals(service.getName(), MetadataMappingService.SERVICE_NAME);
    }

    @Test
    public void testOnAddClusterEntity() throws Exception {
        clusterEntity = buildCluster(CLUSTER_ENTITY_NAME, COLO_NAME, "classification=production");
        configStore.publish(EntityType.CLUSTER, clusterEntity);

        verifyEntityWasAddedToGraph(CLUSTER_ENTITY_NAME, EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE);
        verifyClusterEntityEdges();
    }

    @Test (dependsOnMethods = "testOnAddClusterEntity")
    public void testOnAddFeedEntity() throws Exception {
        Feed impressionsFeed = buildFeed("impression-feed", clusterEntity, "classified-as=Secure",
                "analytics", Storage.TYPE.FILESYSTEM, "jail://global:00/falcon/impression-feed/20140101");
        configStore.publish(EntityType.FEED, impressionsFeed);
        inputFeeds.add(impressionsFeed);
        verifyEntityWasAddedToGraph(impressionsFeed.getName(), EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);
        verifyFeedEntityEdges(impressionsFeed.getName());

        Feed clicksFeed = buildFeed("clicks-feed", clusterEntity, "classified-as=Secure,classified-as=Financial",
                "analytics", Storage.TYPE.FILESYSTEM, "jail://global:00/falcon/clicks-feed/20140101");
        configStore.publish(EntityType.FEED, clicksFeed);
        inputFeeds.add(clicksFeed);
        verifyEntityWasAddedToGraph(clicksFeed.getName(), EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);

        Feed join1Feed = buildFeed("imp-click-join1", clusterEntity, "classified-as=Financial", "reporting,bi",
                Storage.TYPE.FILESYSTEM, "jail://global:00/falcon/imp-click-join1/20140101");
        configStore.publish(EntityType.FEED, join1Feed);
        outputFeeds.add(join1Feed);
        verifyEntityWasAddedToGraph(join1Feed.getName(), EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);

        Feed join2Feed = buildFeed("imp-click-join2", clusterEntity, "classified-as=Secure,classified-as=Financial",
                "reporting,bi", Storage.TYPE.FILESYSTEM, "jail://global:00/falcon/imp-click-join2/20140101");
        configStore.publish(EntityType.FEED, join2Feed);
        outputFeeds.add(join2Feed);
        verifyEntityWasAddedToGraph(join2Feed.getName(), EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);

        Vertex feedVertex = getEntityVertex("imp-click-join2", EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);

        String groupTag = "bi";
        Vertex groupVertex = findVertex(groupTag, RelationshipGraphBuilder.GROUPS_TYPE);
        System.out.println("** groupVertex = " + MetadataMappingService.vertexString(groupVertex));
        for (Edge edge : feedVertex.getEdges(Direction.OUT, RelationshipGraphBuilder.GROUPS_LABEL)) {
            System.out.println("** edge = " + MetadataMappingService.edgeString(edge));
            String name = edge.getVertex(Direction.IN).getProperty("name");
            System.out.println("** name = " + name);
            if (name.equals(groupTag)) {
                System.out.println("*****boom = " + MetadataMappingService.edgeString(edge));
            }
        }

        String tagKey = "classified-as";
        String tagValue = "Secure";
        for (Edge edge : feedVertex.getEdges(Direction.OUT, tagKey)) {
            System.out.println("** edge = " + MetadataMappingService.edgeString(edge));
            String name = edge.getVertex(Direction.IN).getProperty("name");
            System.out.println("** name = " + name);
            if (tagValue.equals(name)) {
                System.out.println("*****boom = " + MetadataMappingService.edgeString(edge));
            }
        }
    }

    @Test (dependsOnMethods = "testOnAddFeedEntity")
    public void testOnAddProcessEntity() throws Exception {
        processEntity = buildProcess(PROCESS_ENTITY_NAME, clusterEntity, "classified-as=Critical");
        addWorkflow(processEntity, WORKFLOW_NAME, WORKFLOW_VERSION);

        for (Feed inputFeed : inputFeeds) {
            addInput(processEntity, inputFeed);
        }

        for (Feed outputFeed : outputFeeds) {
            addOutput(processEntity, outputFeed);
        }

        configStore.publish(EntityType.PROCESS, processEntity);

        verifyEntityWasAddedToGraph(processEntity.getName(), EntityRelationshipGraphBuilder.PROCESS_ENTITY_TYPE);
        verifyProcessEntityEdges();
    }

    @Test (dependsOnMethods = "testOnAddProcessEntity")
    public void testOnAdd() throws Exception {
        service.debug();
        verifyEntityGraph(EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE, "Secure");
    }

    @Test (dependsOnMethods = "testMapLineage")
    public void testOnChange() throws Exception {
        // cannot modify cluster, adding a new cluster
        bcpCluster = buildCluster("bcp-cluster", "east-coast", "classification=bcp");
        configStore.publish(EntityType.CLUSTER, bcpCluster);
        verifyEntityWasAddedToGraph("bcp-cluster", EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE);
    }

    @Test(dependsOnMethods = "testOnChange")
    public void testOnFeedEntityChange() throws Exception {
        Feed oldFeed = inputFeeds.get(0);
        // oldFeed.setTags(null);
        // Feed newFeed = (Feed) oldFeed.copy();
        // modify feed
        Feed newFeed = buildFeed(oldFeed.getName(), clusterEntity,
                "classified-as=Secured,source=data-warehouse", "reporting",
                Storage.TYPE.FILESYSTEM, "jail://global:00/falcon/impression-feed/20140101");

        try {
            configStore.initiateUpdate(newFeed);

            // add cluster
            org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                    new org.apache.falcon.entity.v0.feed.Cluster();
            feedCluster.setName(bcpCluster.getName());
            newFeed.getClusters().getClusters().add(feedCluster);

            configStore.update(EntityType.FEED, newFeed);
        } finally {
            configStore.cleanupUpdateInit();
        }
        verifyUpdatedEdges(newFeed);
    }

    private void verifyUpdatedEdges(Feed newFeed) {
        Vertex feedVertex = getEntityVertex(newFeed.getName(),
                EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);

        // groups
        Edge edge = feedVertex.getEdges(Direction.OUT, RelationshipGraphBuilder.GROUPS_LABEL).iterator().next();
        System.out.println("MetadataMappingService.edgeString(edge) = " + MetadataMappingService.edgeString(edge));
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), "reporting");

        // tags
        edge = feedVertex.getEdges(Direction.OUT, "classified-as").iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), "Secured");
        edge = feedVertex.getEdges(Direction.OUT, "source").iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), "data-warehouse");

        // new cluster
        Iterator<Edge> clusterEdgeIterator = feedVertex.getEdges(Direction.OUT,
                RelationshipGraphBuilder.FEED_CLUSTER_EDGE_LABEL).iterator();
        edge = clusterEdgeIterator.next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), clusterEntity.getName());
        edge = clusterEdgeIterator.next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), bcpCluster.getName());
    }

    @Test(dependsOnMethods = "testOnFeedEntityChange")
    public void testOnProcessEntityChange() throws Exception {
        Process oldProcess = processEntity;
        // oldProcess.setTags(null);
        // Process newProcess = (Process) oldProcess.copy();
        // modify the process
        Process newProcess = buildProcess(oldProcess.getName(), bcpCluster, null);
        addWorkflow(newProcess, WORKFLOW_NAME, "2.0.0");
        addInput(newProcess, inputFeeds.get(0));

        try {
            configStore.initiateUpdate(newProcess);
            configStore.update(EntityType.PROCESS, newProcess);
        } finally {
            configStore.cleanupUpdateInit();
        }
        verifyUpdatedEdges(newProcess);
    }

    private void verifyUpdatedEdges(Process newProcess) {
        Vertex processVertex = getEntityVertex(newProcess.getName(),
                EntityRelationshipGraphBuilder.PROCESS_ENTITY_TYPE);

        // cluster
        Edge edge = processVertex.getEdges(Direction.OUT,
                RelationshipGraphBuilder.PROCESS_CLUSTER_EDGE_LABEL).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), bcpCluster.getName());

        // workflow
        edge = processVertex.getEdges(Direction.OUT,
                RelationshipGraphBuilder.PROCESS_WORKFLOW_EDGE_LABEL).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("version"),
                newProcess.getWorkflow().getVersion());

        // inputs
        edge = processVertex.getEdges(Direction.IN,
                RelationshipGraphBuilder.FEED_PROCESS_EDGE_LABEL).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.OUT).getProperty("name"),
                newProcess.getInputs().getInputs().get(0).getFeed());

        // outputs
        for (Edge e : processVertex.getEdges(Direction.OUT, RelationshipGraphBuilder.PROCESS_FEED_EDGE_LABEL)) {
            Assert.fail("there should not be any edges to output feeds" + e);
        }
    }

    @Test(dependsOnMethods = "testOnAdd")
    public void testMapLineage() throws Exception {

        LineageRecorder.main(getTestMessageArgs());

        service.mapLineage(getTestLineageMetaData());
        service.debug();

        verifyLineageGraph(InstanceRelationshipGraphBuilder.FEED_INSTANCE_TYPE);
    }

    private static Cluster buildCluster(String name, String colo, String tags) {
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setColo(colo);
        cluster.setTags(tags);

        Interfaces interfaces = new Interfaces();
        cluster.setInterfaces(interfaces);

        Interface storage = new Interface();
        storage.setEndpoint("jail://global:00");
        storage.setType(Interfacetype.WRITE);
        cluster.getInterfaces().getInterfaces().add(storage);

        return cluster;
    }

    private static Feed buildFeed(String feedName, Cluster cluster, String tags, String groups,
                                  Storage.TYPE storageType, String uriTemplate) {
        Feed feed = new Feed();
        feed.setName(feedName);
        feed.setTags(tags);
        feed.setGroups(groups);
        feed.setFrequency(Frequency.fromString("hours(1)"));

        org.apache.falcon.entity.v0.feed.Clusters
                clusters = new org.apache.falcon.entity.v0.feed.Clusters();
        feed.setClusters(clusters);
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
        feedCluster.setName(cluster.getName());
        clusters.getClusters().add(feedCluster);

/*
        Validity validity = new Validity();
        validity.setStart(new Date());
        validity.setEnd(new Date());
        feedCluster.setValidity(validity);

        Retention retention = new Retention();
        retention.setAction(ActionType.DELETE);
        retention.setLimit(Frequency.fromString("days(1)"));
        retention.setType(RetentionType.INSTANCE);
        feedCluster.setRetention(retention);
*/

        if (storageType == Storage.TYPE.FILESYSTEM) {
            Locations locations = new Locations();
            feed.setLocations(locations);

            Location location = new Location();
            location.setType(LocationType.DATA);
            location.setPath(uriTemplate);
            feed.getLocations().getLocations().add(location);
        } else {
            CatalogTable table = new CatalogTable();
            table.setUri(uriTemplate);
            feed.setTable(table);
        }

/*
        ACL acl = new ACL();
        acl.setGroup("g");
        acl.setOwner(FALCON_USER);
        acl.setPermission("777");
        feed.setACL(acl);

        Schema schema = new Schema();
        schema.setLocation("blah");
        schema.setProvider("blah");
        feed.setSchema(schema);
*/

        return feed;
    }

    private static Process buildProcess(String processName, Cluster cluster,
                                        String tags) throws Exception {
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

    private static void addWorkflow(Process process, String workflowName, String version) {
        Workflow workflow = new Workflow();
        workflow.setName(workflowName);
        workflow.setVersion(version);
        workflow.setEngine(EngineType.PIG);
        workflow.setPath("/falcon/test/workflow");

        process.setWorkflow(workflow);
    }

    private static void addInput(Process process, Feed feed) {
        if (process.getInputs() == null) {
            process.setInputs(new Inputs());
        }

        Inputs inputs = process.getInputs();
        Input input = new Input();
        input.setFeed(feed.getName());
        inputs.getInputs().add(input);
    }

    private static void addOutput(Process process, Feed feed) {
        if (process.getOutputs() == null) {
            process.setOutputs(new Outputs());
        }

        Outputs outputs = process.getOutputs();
        Output output = new Output();
        output.setFeed(feed.getName());
        outputs.getOutputs().add(output);
    }

    private Vertex findVertex(String name, String type) {
        GraphQuery query = getQuery()
                .has(RelationshipGraphBuilder.NAME_PROPERTY_KEY, name)
                .has(RelationshipGraphBuilder.TYPE_PROPERTY_KEY, type);
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;
    }

    private void verifyEntityWasAddedToGraph(String entityName, String entityType) {
        Vertex entityVertex = getEntityVertex(entityName, entityType);
        Assert.assertNotNull(entityVertex);
        verifyEntityProperties(entityVertex, entityName, entityType);
    }

    private void verifyEntityProperties(Vertex entityVertex, String entityName, String entityType) {
        Assert.assertEquals(entityName, entityVertex.getProperty(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY));
        Assert.assertEquals(entityType, entityVertex.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY));
        Assert.assertNotNull(entityVertex.getProperty(EntityRelationshipGraphBuilder.TIMESTAMP_PROPERTY_KEY));
    }

    private void verifyClusterEntityEdges() {
        Vertex clusterVertex = getEntityVertex(CLUSTER_ENTITY_NAME,
                EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE);

        // verify edge to user vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, EntityRelationshipGraphBuilder.USER_LABEL,
                FALCON_USER, EntityRelationshipGraphBuilder.USER_TYPE);
        // verify edge to colo vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, EntityRelationshipGraphBuilder.CLUSTER_COLO_LABEL,
                COLO_NAME, EntityRelationshipGraphBuilder.COLO_TYPE);
        // verify edge to tags vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, "classification",
                "production", EntityRelationshipGraphBuilder.TAGS_TYPE);
    }

    private void verifyFeedEntityEdges(String feedName) {
        Vertex feedVertex = getEntityVertex(feedName, EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE);

        // verify edge to cluster vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, EntityRelationshipGraphBuilder.FEED_CLUSTER_EDGE_LABEL,
                CLUSTER_ENTITY_NAME, EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE);
        // verify edge to user vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, EntityRelationshipGraphBuilder.USER_LABEL,
                FALCON_USER, EntityRelationshipGraphBuilder.USER_TYPE);
        // verify edge to tags vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, "classified-as",
                "Secure", EntityRelationshipGraphBuilder.TAGS_TYPE);
        // verify edge to group vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, EntityRelationshipGraphBuilder.GROUPS_LABEL,
                "analytics", EntityRelationshipGraphBuilder.GROUPS_TYPE);
    }

    private void verifyProcessEntityEdges() {
        Vertex processVertex = getEntityVertex(PROCESS_ENTITY_NAME,
                EntityRelationshipGraphBuilder.PROCESS_ENTITY_TYPE);

        // verify edge to cluster vertex
        verifyVertexForEdge(processVertex, Direction.OUT, EntityRelationshipGraphBuilder.FEED_CLUSTER_EDGE_LABEL,
                CLUSTER_ENTITY_NAME, EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE);
        // verify edge to user vertex
        verifyVertexForEdge(processVertex, Direction.OUT, EntityRelationshipGraphBuilder.USER_LABEL,
                FALCON_USER, EntityRelationshipGraphBuilder.USER_TYPE);
        // verify edge to tags vertex
        verifyVertexForEdge(processVertex, Direction.OUT, "classified-as",
                "Critical", EntityRelationshipGraphBuilder.TAGS_TYPE);

        // verify edge to inputs vertex
        for (Edge edge : processVertex.getEdges(Direction.OUT,
                EntityRelationshipGraphBuilder.FEED_PROCESS_EDGE_LABEL)) {
            Vertex outVertex = edge.getVertex(Direction.OUT);
            Assert.assertEquals(EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE,
                    outVertex.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY));
            String name = outVertex.getProperty(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY);
            if (!(name.equals("impression-feed") || name.equals("clicks-feed"))) {
                Assert.fail("feed name should have been one of impression-feed or clicks-feed");
            }
        }

        // verify edge to outputs vertex
        for (Edge edge : processVertex.getEdges(Direction.IN,
                EntityRelationshipGraphBuilder.PROCESS_FEED_EDGE_LABEL)) {
            Vertex outVertex = edge.getVertex(Direction.IN);
            Assert.assertEquals(EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE,
                    outVertex.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY));
            String name = outVertex.getProperty(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY);
            if (!(name.equals("imp-click-join1") || name.equals("imp-click-join2"))) {
                Assert.fail("feed name should have been one of imp-click-join1 or imp-click-join2");
            }
        }
    }

    private Vertex getEntityVertex(String entityName, String entityType) {
        GraphQuery entityQuery = getQuery()
                .has(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY, entityName)
                .has(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY, entityType);
        Iterator<Vertex> iterator = entityQuery.vertices().iterator();
        Assert.assertTrue(iterator.hasNext());

        Vertex entityVertex = iterator.next();
        Assert.assertNotNull(entityVertex);

        return entityVertex;
    }

    private void verifyVertexForEdge(Vertex fromVertex, Direction direction, String label,
                                     String expectedName, String expectedType) {
        for (Edge edge : fromVertex.getEdges(direction, label)) {
            Vertex outVertex = edge.getVertex(Direction.IN);
            Assert.assertEquals(
                    outVertex.getProperty(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY), expectedName);
            Assert.assertEquals(
                    outVertex.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY), expectedType);
        }
    }

    private void verifyEntityGraph(String feedType, String classification) {
        System.out.println();
        System.out.println();

        // feeds owned by a user
        List<String> feedNamesOwnedByUser = getFeedsOwnedByAUser(feedType);
        Assert.assertEquals(feedNamesOwnedByUser,
                Arrays.asList("impression-feed", "clicks-feed", "imp-click-join1", "imp-click-join2"));

        System.out.println("--------------------------------------");
        // feeds classified as secure
        verifyFeedsClassifiedAsSecure(feedType);

        System.out.println("--------------------------------------");
        // feeds owned by a user and classified as secure
        verifyFeedsOwnedByUserAndClassification(feedType, classification);
    }

    private List<String> getFeedsOwnedByAUser(String feedType) {
        GraphQuery userQuery = getQuery()
                .has(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY, FALCON_USER)
                .has(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY, EntityRelationshipGraphBuilder.USER_TYPE);

        List<String> feedNames = new ArrayList<String>();
        for (Vertex userVertex : userQuery.vertices()) {
            for (Vertex feed : userVertex.getVertices(Direction.IN, EntityRelationshipGraphBuilder.USER_LABEL)) {
                if (feed.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY).equals(feedType)) {
                    System.out.println(FALCON_USER + " owns -> " + MetadataMappingService.vertexString(feed));
                    feedNames.add(feed.<String>getProperty(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY));
                }
            }
        }

        return feedNames;
    }

    private void verifyFeedsClassifiedAsSecure(String feedType) {
        GraphQuery classQuery = getQuery()
                .has(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY, "Secure")
                .has(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY, EntityRelationshipGraphBuilder.TAGS_TYPE);

        for (Vertex feedVertex : classQuery.vertices()) {
            for (Vertex feed : feedVertex.getVertices(Direction.BOTH, "classified-as")) {
                if (feed.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY).equals(feedType)) {
                    System.out.println(" Secure classification -> " + MetadataMappingService.vertexString(feed));
                }
            }
        }
    }

    private void verifyFeedsOwnedByUserAndClassification(String feedType, String classification) {
        Vertex userVertex = getEntityVertex(FALCON_USER, EntityRelationshipGraphBuilder.USER_TYPE);
        for (Vertex feed : userVertex.getVertices(Direction.IN, EntityRelationshipGraphBuilder.USER_LABEL)) {
            if (feed.getProperty(EntityRelationshipGraphBuilder.TYPE_PROPERTY_KEY).equals(feedType)) {
                for (Vertex classVertex : feed.getVertices(Direction.OUT, "classified-as")) {
                    if (classVertex.getProperty(EntityRelationshipGraphBuilder.NAME_PROPERTY_KEY)
                            .equals(classification)) {
                        System.out.println(classification + " feed owned by falcon-user -> "
                                + MetadataMappingService.vertexString(feed));
                    }
                }
            }
        }
    }

    private void verifyLineageGraph(String feedType) {
        System.out.println();
        System.out.println();

        // feeds owned by a user
        List<String> feedNamesOwnedByUser = getFeedsOwnedByAUser(feedType);
        System.out.println("feedNamesOwnedByUser = " + feedNamesOwnedByUser);
        /*
        Assert.assertEquals(feedNamesOwnedByUser,
                Arrays.asList("impression-feed", "clicks-feed", "imp-click-join1", "imp-click-join2"));
        */

        System.out.println("--------------------------------------");
        // feeds classified as secure
        verifyFeedsClassifiedAsSecure(feedType);

        System.out.println("--------------------------------------");
        // feeds owned by a user and classified as secure
        verifyFeedsOwnedByUserAndClassification(feedType, "Financial");
    }

    private static String[] getTestMessageArgs() {
        return new String[]{
            "-" + LineageRecorder.Arg.NOMINAL_TIME.getOptionName(), NOMINAL_TIME,
            "-" + LineageRecorder.Arg.TIMESTAMP.getOptionName(), NOMINAL_TIME,

            "-" + LineageRecorder.Arg.ENTITY_NAME.getOptionName(), PROCESS_ENTITY_NAME,
            "-" + LineageRecorder.Arg.ENTITY_TYPE.getOptionName(), ("process"),
            "-" + LineageRecorder.Arg.CLUSTER.getOptionName(), CLUSTER_ENTITY_NAME,
            "-" + LineageRecorder.Arg.OPERATION.getOptionName(), "GENERATE",

            "-" + LineageRecorder.Arg.INPUT_FEED_NAMES.getOptionName(), INPUT_FEED_NAMES,
            "-" + LineageRecorder.Arg.INPUT_FEED_PATHS.getOptionName(), INPUT_INSTANCE_PATHS,
            "-" + LineageRecorder.Arg.INPUT_FEED_TYPES.getOptionName(), "FILESYSTEM,FILESYSTEM",

            "-" + LineageRecorder.Arg.FEED_NAMES.getOptionName(), OUTPUT_FEED_NAMES,
            "-" + LineageRecorder.Arg.FEED_INSTANCE_PATHS.getOptionName(), OUTPUT_INSTANCE_PATHS,

            "-" + LineageRecorder.Arg.WORKFLOW_ID.getOptionName(), "workflow-01-00",
            "-" + LineageRecorder.Arg.WORKFLOW_USER.getOptionName(), FALCON_USER,
            "-" + LineageRecorder.Arg.RUN_ID.getOptionName(), "1",
            "-" + LineageRecorder.Arg.STATUS.getOptionName(), "SUCCEEDED",
            "-" + LineageRecorder.Arg.WF_ENGINE_URL.getOptionName(), "http://localhost:11000/oozie",
            "-" + LineageRecorder.Arg.USER_SUBFLOW_ID.getOptionName(), "userflow@wf-id",
            "-" + LineageRecorder.Arg.USER_WORKFLOW_NAME.getOptionName(), WORKFLOW_NAME,
            "-" + LineageRecorder.Arg.USER_WORKFLOW_VERSION.getOptionName(), WORKFLOW_VERSION,
            "-" + LineageRecorder.Arg.USER_WORKFLOW_ENGINE.getOptionName(), EngineType.PIG.name(),

            "-" + LineageRecorder.Arg.LOG_DIR.getOptionName(), LOGS_DIR,
        };
    }

    private static Map<String, String> getTestLineageMetaData() {
        Map<String, String> lineage = new HashMap<String, String>();
        lineage.put(LineageRecorder.Arg.NOMINAL_TIME.getOptionName(), NOMINAL_TIME);
        lineage.put(LineageRecorder.Arg.TIMESTAMP.getOptionName(), NOMINAL_TIME);

        lineage.put(LineageRecorder.Arg.ENTITY_NAME.getOptionName(), PROCESS_ENTITY_NAME);
        lineage.put(LineageRecorder.Arg.ENTITY_TYPE.getOptionName(), "process");
        lineage.put(LineageRecorder.Arg.CLUSTER.getOptionName(), CLUSTER_ENTITY_NAME);
        lineage.put(LineageRecorder.Arg.OPERATION.getOptionName(), "GENERATE");

        lineage.put(LineageRecorder.Arg.INPUT_FEED_NAMES.getOptionName(), INPUT_FEED_NAMES);
        lineage.put(LineageRecorder.Arg.INPUT_FEED_PATHS.getOptionName(), INPUT_INSTANCE_PATHS);

        lineage.put(LineageRecorder.Arg.FEED_NAMES.getOptionName(), OUTPUT_FEED_NAMES);
        lineage.put(LineageRecorder.Arg.FEED_INSTANCE_PATHS.getOptionName(), OUTPUT_INSTANCE_PATHS);

        lineage.put(LineageRecorder.Arg.WORKFLOW_ID.getOptionName(), "workflow-01-00");
        lineage.put(LineageRecorder.Arg.WORKFLOW_USER.getOptionName(), FALCON_USER);
        lineage.put(LineageRecorder.Arg.RUN_ID.getOptionName(), "1");
        lineage.put(LineageRecorder.Arg.STATUS.getOptionName(), "SUCCEEDED");
        lineage.put(LineageRecorder.Arg.WF_ENGINE_URL.getOptionName(), "http://localhost:11000/oozie");
        lineage.put(LineageRecorder.Arg.USER_SUBFLOW_ID.getOptionName(), "userflow@wf-id");
        lineage.put(LineageRecorder.Arg.USER_WORKFLOW_NAME.getOptionName(), WORKFLOW_NAME);
        lineage.put(LineageRecorder.Arg.USER_WORKFLOW_VERSION.getOptionName(), WORKFLOW_VERSION);
        lineage.put(LineageRecorder.Arg.USER_WORKFLOW_ENGINE.getOptionName(), EngineType.PIG.name());

        lineage.put(LineageRecorder.Arg.LOG_DIR.getOptionName(), LOGS_DIR);
        return lineage;
    }

    private void cleanupGraphStore(KeyIndexableGraph graph) {
        for (Edge edge : graph.getEdges()) {
            graph.removeEdge(edge);
        }

        for (Vertex vertex : graph.getVertices()) {
            graph.removeVertex(vertex);
        }

        graph.shutdown();
    }

    private static void cleanupConfigurationStore(ConfigurationStore store) throws Exception {
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }
}
