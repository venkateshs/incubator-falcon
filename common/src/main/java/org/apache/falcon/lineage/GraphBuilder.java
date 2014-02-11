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

package org.apache.falcon.lineage;

import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Graph builder.
 */
public class GraphBuilder {

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

    public void addLineageToGraph(Map<String, String> lineageMetadata) {
        Vertex processInstance = addProcessInstance(lineageMetadata);
        addOutputFeedInstances(lineageMetadata, processInstance);
        addInputFeedInstances(lineageMetadata, processInstance);
    }

    private Vertex addProcessInstance(Map<String, String> lineageMetadata) {
        String processInstanceName = getProcessInstance(lineageMetadata.get("nominalTime"),
                lineageMetadata.get("entityName"));

        Vertex processInstance = findOrCreateVertex(processInstanceName, "process-instance");
        processInstance.setProperty("name", processInstanceName);
        processInstance.setProperty("type", "process-instance");
        processInstance.setProperty("time", lineageMetadata.get("timeStamp"));

        addWorkflowInstance(processInstance, lineageMetadata);

        addInstanceToEntity(processInstance,
                lineageMetadata.get("entityName"), "process", "instance of");
        addInstanceToEntity(processInstance,
                lineageMetadata.get("cluster"), "cluster", "runs on");

        return processInstance;
    }

    private void addWorkflowInstance(Vertex processInstance, Map<String, String> lineageMetadata) {
        String workflowId = lineageMetadata.get("workflowId");
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

    private void addInstanceToEntity(Vertex instance, String entityName, String entityType,
                                     String edgeLabel) {
        Vertex entity = findOrCreateVertex(entityName, entityType);
        instance.addEdge(edgeLabel, entity);
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
