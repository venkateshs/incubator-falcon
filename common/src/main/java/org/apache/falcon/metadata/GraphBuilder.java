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

import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.metadata.LineageRecorder.Arg;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Graph builder.
 */
public class GraphBuilder implements ConfigurationChangeListener {

    // vertex types
    private static final String USER_TYPE = "user";
    private static final String COLO_TYPE = "data-center";
    private static final String TAGS_TYPE = "classification";
    private static final String GROUPS_TYPE = "group";

    // entity vertex types
    private static final String CLUSTER_ENTITY_TYPE = "cluster-entity";
    private static final String FEED_ENTITY_TYPE = "feed-entity";
    private static final String PROCESS_ENTITY_TYPE = "process-entity";
    private static final String USER_WORKFLOW_TYPE = "user-workflow";

    // instance vertex types
    private static final String FEED_INSTANCE_TYPE = "feed-instance";
    private static final String PROCESS_INSTANCE_TYPE = "process-instance";
    private static final String WORKFLOW_INSTANCE_TYPE = "workflow-instance";

    // edge labels
    private static final String USER_LABEL = "owned by";
    private static final String CLUSTER_COLO_LABEL = "collocated";
    private static final String GROUPS_LABEL = "grouped as";

    // entity edge labels
    private static final String FEED_CLUSTER_EDGE_LABEL = "stored";
    private static final String PROCESS_CLUSTER_EDGE_LABEL = "processed";
    private static final String FEED_PROCESS_EDGE_LABEL = "input";
    private static final String PROCESS_FEED_EDGE_LABEL = "output";
    private static final String PROCESS_WORKFLOW_EDGE_LABEL = "executes";

    // instance edge labels
    private static final String INSTANCE_ENTITY_EDGE_LABEL = "instance of";

    private final KeyIndexableGraph graph;

    public GraphBuilder() throws URISyntaxException {
        URI uri = GraphBuilder.class.getResource("/graph.properties").toURI();
        System.out.println("uri = " + uri);

        File confFile = new File(uri);
        System.out.println("confFile.toString() = " + confFile.toString());
        System.out.println("confFile.exists() = " + confFile.exists());
        System.out.println("confFile.isFile() = " + confFile.isFile());

        // graph = (KeyIndexableGraph) GraphFactory.open(confFile.toString());
        graph = TitanFactory.open(confFile.toString());

        createIndicesForVertexKeys();
    }

    private void createIndicesForVertexKeys() {
        graph.createKeyIndex("name", Vertex.class);
        graph.createKeyIndex("type", Vertex.class);
        graph.createKeyIndex("version", Vertex.class);
    }

    @Override
    public void onAdd(Entity entity, boolean ignoreFailure) throws FalconException {
        switch (entity.getEntityType()) {
            case CLUSTER:
                addClusterEntity((Cluster) entity);
                break;

            case FEED:
                addFeedEntity((Feed) entity);
                break;

            case PROCESS:
                addProcessEntity((org.apache.falcon.entity.v0.process.Process) entity);
                break;

            default:
        }
    }

    private void addClusterEntity(Cluster clusterEntity) {
        Vertex clusterVertex = addVertex(clusterEntity.getName(), CLUSTER_ENTITY_TYPE);
        addTimestamp(clusterVertex);

        addUser(clusterVertex);
        addColo(clusterEntity.getColo(), clusterVertex);
        addDataClassification(clusterEntity.getTags(), clusterVertex);
    }

    private void addTimestamp(Vertex vertex) {
        vertex.setProperty("timestamp", System.currentTimeMillis());
    }

    private void addTimestamp(Vertex vertex, String timestamp) {
        vertex.setProperty("timestamp", timestamp);
    }

    private void addUser(Vertex fromVertex) {
        Vertex userVertex = addVertex(CurrentUser.getUser(), USER_TYPE);
        fromVertex.addEdge(USER_LABEL, userVertex);
    }

    private void addColo(String colo, Vertex fromVertex) {
        Vertex coloVertex = addVertex(colo, COLO_TYPE);
        fromVertex.addEdge(CLUSTER_COLO_LABEL, coloVertex);
    }

    private void addDataClassification(String classification, Vertex entityVertex) {
        if (classification == null || classification.length() == 0) {
            return;
        }

        String[] tags = classification.split(",");
        for (String tag : tags) {
            int index = tag.indexOf("=");
            String tagKey = tag.substring(0, index);
            String tagValue = tag.substring(index + 1, tag.length());

            Vertex tagValueVertex = addVertex(tagValue, TAGS_TYPE);
            entityVertex.addEdge(tagKey, tagValueVertex);
        }
    }

    private Vertex addVertex(String name, String type) {
        Vertex vertex = findVertex(name, type);
        if (vertex == null) {
            vertex = buildVertex(name, type);
        }

        return vertex;
    }

    private Vertex findVertex(String name, String type) {
        GraphQuery query = graph.query()
                .has("name", name)
                .has("type", type);
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;
    }

    private Vertex buildVertex(String name, String type) {
        Vertex vertex = graph.addVertex(null);
        vertex.setProperty("name", name);
        vertex.setProperty("type", type);

        return vertex;
    }

    private void addFeedEntity(Feed feedEntity) {
        Vertex feedVertex = addVertex(feedEntity.getName(), FEED_ENTITY_TYPE);
        addTimestamp(feedVertex);

        addUser(feedVertex);
        addDataClassification(feedEntity.getTags(), feedVertex);
        addGroups(feedEntity.getGroups(), feedVertex);

        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feedEntity.getClusters().getClusters()) {
            addCluster(feedCluster.getName(), feedVertex, FEED_CLUSTER_EDGE_LABEL);
        }
    }

    private void addCluster(String clusterName, Vertex fromVertex,
                            String edgeLabel) {
        Vertex clusterVertex = findVertex(clusterName, CLUSTER_ENTITY_TYPE);
        if (clusterVertex == null) {
            throw new IllegalStateException("Cluster entity must exist: " + clusterName);
        }

        fromVertex.addEdge(edgeLabel, clusterVertex);
    }

    private void addGroups(String groups, Vertex entityVertex) {
        if (groups == null || groups.length() == 0) {
            return;
        }

        String[] groupTags = groups.split(",");
        for (String groupTag : groupTags) {
            Vertex groupVertex = addVertex(groupTag, GROUPS_TYPE);
            entityVertex.addEdge(GROUPS_LABEL, groupVertex);
        }
    }

    private void addProcessEntity(Process processEntity) {
        Vertex processVertex = addVertex(processEntity.getName(), PROCESS_ENTITY_TYPE);
        addTimestamp(processVertex);

        addUser(processVertex);
        addDataClassification(processEntity.getTags(), processVertex);

        for (org.apache.falcon.entity.v0.process.Cluster processCluster : processEntity.getClusters().getClusters()) {
            addCluster(processCluster.getName(), processVertex, PROCESS_CLUSTER_EDGE_LABEL);
        }

        addInputFeeds(processEntity.getInputs(), processVertex);
        addOutputFeeds(processEntity.getOutputs(), processVertex);
        addWorkflow(processEntity.getWorkflow(), processVertex);
    }

    private void addInputFeeds(Inputs inputs, Vertex processVertex) {
        if (inputs == null) {
            return;
        }

        for (Input input : inputs.getInputs()) {
            addFeed(input.getFeed(), processVertex, FEED_PROCESS_EDGE_LABEL);
        }
    }

    private void addOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            addFeed(output.getFeed(), processVertex, PROCESS_FEED_EDGE_LABEL);
        }
    }

    private void addFeed(String feedName, Vertex processVertex, String edgeDirection) {
        Vertex feedVertex = findVertex(feedName, FEED_ENTITY_TYPE);
        if (feedVertex == null) {
            throw new IllegalStateException("Feed entity must exist: " + feedName);
        }

        Edge edge = edgeDirection.equals(FEED_PROCESS_EDGE_LABEL)
                ? feedVertex.addEdge(edgeDirection, processVertex)
                : processVertex.addEdge(edgeDirection, feedVertex);
        System.out.println("edge = " + edge);
    }

    private void addWorkflow(Workflow workflow, Vertex processVertex) {
        Vertex workflowVertex = addVertex(workflow.getName(), USER_WORKFLOW_TYPE);
        addTimestamp(workflowVertex);
        workflowVertex.setProperty("version", workflow.getVersion());
        workflowVertex.setProperty("engine", workflow.getEngine().value());

        processVertex.addEdge(PROCESS_WORKFLOW_EDGE_LABEL, workflowVertex);

        addUser(workflowVertex); // is this necessary?
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // do nothing, we'd leave the deleted entities as is for historical purposes
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        // todo hmmm, need to address this
    }

    public void addLineageToGraph(Map<String, String> lineageMetadata) {
        Vertex processInstance = addProcessInstance(lineageMetadata);
        addOutputFeedInstances(lineageMetadata, processInstance);
        addInputFeedInstances(lineageMetadata, processInstance);
    }

    private Vertex addProcessInstance(Map<String, String> lineageMetadata) {
        String entityName = lineageMetadata.get(Arg.ENTITY_NAME.getOptionName());
        String processInstanceName = getProcessInstance(
                lineageMetadata.get(Arg.NOMINAL_TIME.getOptionName()), entityName);

        Vertex processInstance = addVertex(processInstanceName, PROCESS_INSTANCE_TYPE);
        String timestamp = lineageMetadata.get(Arg.TIMESTAMP.getOptionName());
        addTimestamp(processInstance, timestamp);

        addWorkflowInstance(processInstance, lineageMetadata);

        addInstanceToEntity(processInstance, entityName,
                PROCESS_ENTITY_TYPE, INSTANCE_ENTITY_EDGE_LABEL);
        addInstanceToEntity(processInstance,
                lineageMetadata.get(Arg.CLUSTER.getOptionName()), CLUSTER_ENTITY_TYPE, "runs on");

        return processInstance;
    }

    private void addWorkflowInstance(Vertex processInstance, Map<String, String> lineageMetadata) {
        String workflowId = lineageMetadata.get(Arg.WORKFLOW_ID.getOptionName());
        Vertex processWorkflowInstance = findOrCreateVertex(workflowId, "workflow-instance");

        processWorkflowInstance.setProperty("name", workflowId);
        processWorkflowInstance.setProperty("type", "workflow-instance");
        processWorkflowInstance.setProperty("time", lineageMetadata.get("timeStamp"));
        processWorkflowInstance.setProperty("version", "1.0");

        processInstance.addEdge("executes", processWorkflowInstance);
    }

    private String getProcessInstance(String nominalTime, String entityName) {
        // todo
        return nominalTime;
    }

    private Vertex findOrCreateVertex(String name, String type) {
        GraphQuery query = graph.query()
                .has("name", name)
                .has("type", type);
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : graph.addVertex(null);
    }

    private void addInstanceToEntity(Vertex instance, String entityName,
                                     String entityType, String edgeLabel) {
        Vertex entityVertex = findVertex(entityName, entityType);
        if (entityVertex == null) {
            throw new IllegalStateException(entityType + " entity must exist " + entityName);
        }

        instance.addEdge(edgeLabel, entityVertex);
    }

    private void addOutputFeedInstances(Map<String, String> lineageMetadata,
                                        Vertex processInstance) {
        String[] outputFeedNames = lineageMetadata.get("feedNames").split(",");
        String[] outputFeedInstancePaths = lineageMetadata.get("feedInstancePaths").split(",");

        addFeedInstances(outputFeedNames, outputFeedInstancePaths,
                processInstance, "output", lineageMetadata);
    }

    private void addInputFeedInstances(Map<String, String> lineageMetadata,
                                       Vertex processInstance) {
        String[] inputFeedNames = lineageMetadata.get("falconInputFeeds").split(",");
        String[] inputFeedInstancePaths = lineageMetadata.get("falconInPaths").split(",");

        addFeedInstances(inputFeedNames, inputFeedInstancePaths,
                processInstance, "input", lineageMetadata);
    }

    private void addFeedInstances(String[] feedNames, String[] feedInstancePaths,
                                  Vertex processInstance, String label,
                                  Map<String, String> lineageMetadata) {
        for (int index = 0; index < feedNames.length; index++) {
            String feedName = feedNames[index];
            String feedInstancePath = feedInstancePaths[index];

            String instance = getFeedInstance(feedName, feedInstancePath);
            Vertex feedInstance = findOrCreateVertex(instance, "feed-instance");
            feedInstance.setProperty("name", instance);
            feedInstance.setProperty("type", "feed-instance");
            feedInstance.setProperty("time", lineageMetadata.get("timeStamp"));

            Edge edge = label.equals("input")
                    ? feedInstance.addEdge("input", processInstance)
                    : processInstance.addEdge("output", feedInstance);
            System.out.println("edge = " + edge);

            addInstanceToEntity(feedInstance, feedName, "feed", "instance of");
            addInstanceToEntity(feedInstance,
                    lineageMetadata.get("cluster"), "cluster", "stored in");
        }
    }

    private String getFeedInstance(String feedName, String feedInstancePath) {
        // todo - this is yuck
        return feedInstancePath;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> lineage = getTestData();

        GraphBuilder builder = new GraphBuilder();
        builder.addLineageToGraph(lineage);

        builder.debug();
    }

    private static Map<String, String> getTestData() {
        Map<String, String> lineage = new HashMap<String, String>();
        lineage.put("nominalTime", "2014-01-01-01-00");
        lineage.put("timeStamp", "2014-01-01-01-00");

        lineage.put("entityName", "test-process-entity");
        lineage.put("entityType", "process");
        lineage.put("cluster", "test-cluster-entity");
        lineage.put("operation", "GENERATE");

        lineage.put("workflowUser", "falcon-user");
        lineage.put("workflowEngineUrl", "http://localhost:11000/oozie");
        lineage.put("subflowId", "userflow@wf-id");
        lineage.put("userWorkflowEngine", "oozie");
        lineage.put("workflowId", "workflow-01-00");
        lineage.put("runId", "1");
        lineage.put("status", "SUCCEEDED");

        lineage.put("falconInputFeeds",  "in-click-logs,in-raw-logs");
        lineage.put("falconInPaths",
                "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20");

        lineage.put("feedNames",  "out-click-logs,out-raw-logs");
        lineage.put("feedInstancePaths",
                "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20");

        lineage.put("logDir", "target/log");
        return lineage;
    }

    private void debug() {
        System.out.println("Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
            System.out.println(vertex);
            System.out.println(vertex.getProperty("name") + ", " + vertex.getProperty("type"));
        }

        System.out.println("Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(edge);
        }
    }
}
