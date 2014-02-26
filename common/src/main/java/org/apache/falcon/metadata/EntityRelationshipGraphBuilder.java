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
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;

import java.util.ArrayList;
import java.util.List;

/**
 * Entity Metadata relationship mapping helper.
 */
public class EntityRelationshipGraphBuilder extends RelationshipGraphBuilder {

    // entity vertex types
    public static final String CLUSTER_ENTITY_TYPE = "cluster-entity";
    public static final String FEED_ENTITY_TYPE = "feed-entity";
    public static final String PROCESS_ENTITY_TYPE = "process-entity";
    public static final String USER_WORKFLOW_TYPE = "user-workflow";

    public EntityRelationshipGraphBuilder(KeyIndexableGraph graph) {
        super(graph);
    }

    public void addClusterEntity(Cluster clusterEntity) {
        Vertex clusterVertex = addVertex(clusterEntity.getName(),
                CLUSTER_ENTITY_TYPE, System.currentTimeMillis());

        addUser(clusterVertex);
        addColo(clusterEntity.getColo(), clusterVertex);
        addDataClassification(clusterEntity.getTags(), clusterVertex);
    }

    public void addColo(String colo, Vertex fromVertex) {
        Vertex coloVertex = addVertex(colo, COLO_TYPE);
        fromVertex.addEdge(CLUSTER_COLO_LABEL, coloVertex);
    }

    public void addFeedEntity(Feed feed) {
        Vertex feedVertex = addVertex(feed.getName(),
                FEED_ENTITY_TYPE, System.currentTimeMillis());

        addUser(feedVertex);
        addDataClassification(feed.getTags(), feedVertex);
        addGroups(feed.getGroups(), feedVertex);

        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
            addEdgeToCluster(feedCluster.getName(), feedVertex, FEED_CLUSTER_EDGE_LABEL);
        }
    }

    public void addEdgeToCluster(String clusterName, Vertex fromVertex, String edgeLabel) {
        Vertex clusterVertex = findVertex(clusterName, CLUSTER_ENTITY_TYPE);
        if (clusterVertex == null) {
            throw new IllegalStateException("Cluster entity must exist: " + clusterName);
        }

        fromVertex.addEdge(edgeLabel, clusterVertex);
    }

    public void addProcessEntity(org.apache.falcon.entity.v0.process.Process process) {
        Vertex processVertex = addVertex(process.getName(),
                PROCESS_ENTITY_TYPE, System.currentTimeMillis());

        addUser(processVertex);
        addDataClassification(process.getTags(), processVertex);

        for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            addEdgeToCluster(cluster.getName(), processVertex, PROCESS_CLUSTER_EDGE_LABEL);
        }

        addInputFeeds(process.getInputs(), processVertex);
        addOutputFeeds(process.getOutputs(), processVertex);
        addWorkflow(process.getWorkflow(), processVertex);
    }

    public void addInputFeeds(Inputs inputs, Vertex processVertex) {
        if (inputs == null) {
            return;
        }

        for (Input input : inputs.getInputs()) {
            addFeed(input.getFeed(), processVertex, FEED_PROCESS_EDGE_LABEL);
        }
    }

    public void addOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            addFeed(output.getFeed(), processVertex, PROCESS_FEED_EDGE_LABEL);
        }
    }

    public void addFeed(String feedName, Vertex processVertex, String edgeDirection) {
        Vertex feedVertex = findVertex(feedName, FEED_ENTITY_TYPE);
        if (feedVertex == null) {
            throw new IllegalStateException("Feed entity must exist: " + feedName);
        }

        addProcessFeedEdge(processVertex, feedVertex, edgeDirection);
    }

    public void addWorkflow(Workflow workflow, Vertex processVertex) {
        Vertex workflowVertex = addVertex(workflow.getName(), USER_WORKFLOW_TYPE,
                System.currentTimeMillis());
        workflowVertex.setProperty(VERSION_PROPERTY_KEY, workflow.getVersion());
        workflowVertex.setProperty("engine", workflow.getEngine().value());

        processVertex.addEdge(PROCESS_WORKFLOW_EDGE_LABEL, workflowVertex);

        addUser(workflowVertex); // is this necessary?
    }

    public void updateFeedEntity(Feed oldFeed, Feed newFeed) {
        Vertex feedEntityVertex = findVertex(oldFeed.getName(), FEED_ENTITY_TYPE);
        if (feedEntityVertex == null) {
            throw new IllegalStateException(oldFeed.getName() + " entity must exist.");
        }

        updateDataClassification(oldFeed.getTags(), newFeed.getTags(), feedEntityVertex);
        updateGroups(oldFeed.getGroups(), newFeed.getGroups(), feedEntityVertex);
        updateFeedClusters(oldFeed.getClusters().getClusters(),
                newFeed.getClusters().getClusters(), feedEntityVertex);
    }

    public void updateProcessEntity(Process oldProcess, Process newProcess) {
        Vertex processEntityVertex = findVertex(oldProcess.getName(), PROCESS_ENTITY_TYPE);
        if (processEntityVertex == null) {
            throw new IllegalStateException(oldProcess.getName() + " entity must exist.");
        }

        updateDataClassification(oldProcess.getTags(), newProcess.getTags(), processEntityVertex);
        updateProcessClusters(oldProcess.getClusters().getClusters(),
                newProcess.getClusters().getClusters(), processEntityVertex);
        updateProcessWorkflow(oldProcess.getWorkflow(), newProcess.getWorkflow(), processEntityVertex);
        updateProcessInputs(oldProcess.getInputs(), newProcess.getInputs(), processEntityVertex);
        updateProcessOutputs(oldProcess.getOutputs(), newProcess.getOutputs(), processEntityVertex);
    }

    public void updateDataClassification(String oldClassification, String newClassification,
                                         Vertex entityVertex) {
        if (areSame(oldClassification, newClassification)) {
            return;
        }

        removeDataClassification(oldClassification, entityVertex);
        addDataClassification(newClassification, entityVertex);
    }

    private void removeDataClassification(String classification, Vertex entityVertex) {
        if (classification == null || classification.length() == 0) {
            return;
        }

        String[] oldTags = classification.split(",");
        for (String oldTag : oldTags) {
            int index = oldTag.indexOf("=");
            String tagKey = oldTag.substring(0, index);
            String tagValue = oldTag.substring(index + 1, oldTag.length());

            // remove the edge and not the vertex since instances are pointing to this vertex
            removeEdge(entityVertex, tagValue, tagKey);
/*
            for (Edge edge : entityVertex.getEdges(Direction.OUT, tagKey)) {
                if (tagValue.equals(edge.getVertex(Direction.IN).getProperty(NAME_PROPERTY_KEY))) {
                    System.out.println("*****removing = " + MetadataMappingService
                            .edgeString(edge));
                    getGraph().removeEdge(edge);
                }
            }
*/
        }
    }

    public void updateGroups(String oldGroups, String newGroups, Vertex entityVertex) {
        if (areSame(oldGroups, newGroups)) {
            return;
        }

        removeGroups(oldGroups, entityVertex);
        addGroups(newGroups, entityVertex);
    }

    private void removeGroups(String groups, Vertex entityVertex) {
        if (groups == null || groups.length() == 0) {
            return;
        }

        String[] oldGroupTags = groups.split(",");
        for (String groupTag : oldGroupTags) {
            removeEdge(entityVertex, groupTag, RelationshipGraphBuilder.GROUPS_LABEL);
/*
            for (Edge edge : entityVertex.getEdges(Direction.OUT, RelationshipGraphBuilder.GROUPS_LABEL)) {
                if (groupTag.equals(edge.getVertex(Direction.IN).getProperty(NAME_PROPERTY_KEY))) {
                    System.out.println("*****removing = " + MetadataMappingService.edgeString(edge));
                    getGraph().removeEdge(edge);
                }
            }
*/
        }
    }

    public static boolean areSame(String oldValue, String newValue) {
        return oldValue == null && newValue == null
                || oldValue != null && newValue != null && oldValue.equals(newValue);
    }

    public void updateFeedClusters(List<org.apache.falcon.entity.v0.feed.Cluster> oldClusters,
                                   List<org.apache.falcon.entity.v0.feed.Cluster> newClusters,
                                   Vertex feedEntityVertex) {
        if (areFeedClustersSame(oldClusters, newClusters)) {
            return;
        }

        // remove edges to old clusters
        for (org.apache.falcon.entity.v0.feed.Cluster oldCuster : oldClusters) {
            // removeEdgeToCluster(oldCuster.getName(), feedEntityVertex, FEED_CLUSTER_EDGE_LABEL);
            removeEdge(feedEntityVertex, oldCuster.getName(), FEED_CLUSTER_EDGE_LABEL);
        }

        // add edges to new clusters
        for (org.apache.falcon.entity.v0.feed.Cluster newCluster : newClusters) {
            addEdgeToCluster(newCluster.getName(), feedEntityVertex, FEED_CLUSTER_EDGE_LABEL);
        }
    }

    public boolean areFeedClustersSame(List<org.apache.falcon.entity.v0.feed.Cluster> oldClusters,
                                       List<org.apache.falcon.entity.v0.feed.Cluster> newClusters) {
        if (oldClusters.size() != newClusters.size()) {
            return false;
        }

        List<String> oldClusterNames = getFeedClusterNames(oldClusters);
        List<String> newClusterNames = getFeedClusterNames(newClusters);

        return oldClusterNames.size() == newClusterNames.size()
                && oldClusterNames.containsAll(newClusterNames)
                && newClusterNames.containsAll(oldClusterNames);
    }

    public List<String> getFeedClusterNames(List<org.apache.falcon.entity.v0.feed.Cluster> clusters) {
        List<String> clusterNames = new ArrayList<String>(clusters.size());
        for (org.apache.falcon.entity.v0.feed.Cluster cluster : clusters) {
            clusterNames.add(cluster.getName());
        }

        return clusterNames;
    }

/*
    private static boolean areSame(List<String> oldClusterNames, List<String> newClusterNames) {
        System.out.println("oldClusterNames = " + oldClusterNames);
        Collections.sort(oldClusterNames);
        String oldClusters = oldClusterNames.toString();
        System.out.println("oldClusters = " + oldClusters);

        System.out.println("newClusterNames = " + newClusterNames);
        Collections.sort(newClusterNames);
        String newClusters = newClusterNames.toString();
        System.out.println("newClusters = " + newClusters);

        return oldClusters.equals(newClusters);
    }
*/

    public void removeEdge(Vertex fromVertex, Object toVertexName, String edgeLabel) {
        for (Edge edge : fromVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (edge.getVertex(Direction.IN).getProperty(NAME_PROPERTY_KEY).equals(toVertexName)) {
                // remove the edge and not the vertex since instances are pointing to this vertex
                System.out.println("*****removing = " + MetadataMappingService.edgeString(edge));
                getGraph().removeEdge(edge);
            }
        }
    }

    public void updateProcessClusters(List<org.apache.falcon.entity.v0.process.Cluster> oldClusters,
                                      List<org.apache.falcon.entity.v0.process.Cluster> newClusters,
                                      Vertex processEntityVertex) {
        if (areProcessClustersSame(oldClusters, newClusters)) {
            return;
        }

        // remove old clusters
        for (org.apache.falcon.entity.v0.process.Cluster oldCuster : oldClusters) {
            // removeEdgeToCluster(oldCuster.getName(), processEntityVertex,
            // PROCESS_CLUSTER_EDGE_LABEL);
            removeEdge(processEntityVertex, oldCuster.getName(), PROCESS_CLUSTER_EDGE_LABEL);
        }

        // add new clusters
        for (org.apache.falcon.entity.v0.process.Cluster newCluster : newClusters) {
            addEdgeToCluster(newCluster.getName(), processEntityVertex, PROCESS_CLUSTER_EDGE_LABEL);
        }
    }

    public boolean areProcessClustersSame(List<org.apache.falcon.entity.v0.process.Cluster> oldClusters,
                                          List<org.apache.falcon.entity.v0.process.Cluster> newClusters) {
        if (oldClusters.size() != newClusters.size()) {
            return false;
        }

        List<String> oldClusterNames = getProcessClusterNames(oldClusters);
        List<String> newClusterNames = getProcessClusterNames(newClusters);

        return oldClusterNames.size() == newClusterNames.size()
                && oldClusterNames.containsAll(newClusterNames)
                && newClusterNames.containsAll(oldClusterNames);
    }

    public List<String> getProcessClusterNames(List<org.apache.falcon.entity.v0.process.Cluster> clusters) {
        List<String> clusterNames = new ArrayList<String>(clusters.size());
        for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
            clusterNames.add(cluster.getName());
        }

        return clusterNames;
    }

/*
    private void removeEdgeToCluster(String oldCusterName, Vertex fromEntityVertex, String edgeLabel) {
//        Vertex clusterVertex = findVertex(oldCusterName, CLUSTER_ENTITY_TYPE);
//        if (clusterVertex == null) {
//            throw new IllegalStateException("Cluster entity must exist: " + oldCusterName);
//        }

        // remove the edge and not the vertex since instances are pointing to this vertex
        for (Edge edge : fromEntityVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (oldCusterName.equals(edge.getVertex(Direction.IN).getProperty(NAME_PROPERTY_KEY))) {
                System.out.println("*****removing = " + MetadataMappingService.edgeString(edge));
                getGraph().removeEdge(edge);
            }
        }
    }
*/

    public void updateProcessWorkflow(Workflow oldWorkflow, Workflow newWorkflow,
                                       Vertex processEntityVertex) {
        if (areSame(oldWorkflow, newWorkflow)) {
            return;
        }

        removeEdge(processEntityVertex, oldWorkflow.getName(),
                RelationshipGraphBuilder.PROCESS_WORKFLOW_EDGE_LABEL);

        addWorkflow(newWorkflow, processEntityVertex);
    }

    public boolean areSame(Workflow oldWorkflow, Workflow newWorkflow) {
        return areSame(oldWorkflow.getName(), newWorkflow.getName())
                && areSame(oldWorkflow.getVersion(), newWorkflow.getVersion())
                && areSame(oldWorkflow.getEngine().value(), newWorkflow.getEngine().value());
    }

    private void updateProcessInputs(Inputs oldProcessInputs, Inputs newProcessInputs,
                                     Vertex processEntityVertex) {
        if (areSame(oldProcessInputs, newProcessInputs)) {
            return;
        }

        removeInputFeeds(oldProcessInputs, processEntityVertex);
        addInputFeeds(newProcessInputs, processEntityVertex);
    }

    public static boolean areSame(Inputs oldProcessInputs, Inputs newProcessInputs) {
        if (oldProcessInputs == null && newProcessInputs == null
                || oldProcessInputs == null || newProcessInputs == null
                || oldProcessInputs.getInputs().size() != newProcessInputs.getInputs().size()) {
            return false;
        }

        List<Input> oldInputs = oldProcessInputs.getInputs();
        List<Input> newInputs = newProcessInputs.getInputs();

        return oldInputs.size() == newInputs.size()
                && oldInputs.containsAll(newInputs)
                && newInputs.containsAll(oldInputs);
    }

    public void removeInputFeeds(Inputs inputs, Vertex processVertex) {
        if (inputs == null) {
            return;
        }

        for (Input input : inputs.getInputs()) {
            removeProcessFeedEdge(processVertex, input.getFeed(), FEED_PROCESS_EDGE_LABEL);
        }
    }

    public void removeOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            removeProcessFeedEdge(processVertex, output.getFeed(), PROCESS_FEED_EDGE_LABEL);
        }
    }

    public void removeProcessFeedEdge(Vertex processVertex, String feedName, String edgeLabel) {
        Vertex feedVertex = findVertex(feedName, FEED_ENTITY_TYPE);
        if (feedVertex == null) {
            throw new IllegalStateException("Feed entity must exist: " + feedName);
        }

        if (edgeLabel.equals(FEED_PROCESS_EDGE_LABEL)) {
            // feedVertex.addEdge(edgeLabel, processVertex);
            removeEdge(feedVertex,
                    processVertex.getProperty(RelationshipGraphBuilder.NAME_PROPERTY_KEY), edgeLabel);
        } else {
            // processVertex.addEdge(edgeLabel, feedVertex);
            removeEdge(processVertex,
                    feedVertex.getProperty(RelationshipGraphBuilder.NAME_PROPERTY_KEY), edgeLabel);
        }
    }

    private void updateProcessOutputs(Outputs oldProcessOutputs, Outputs newProcessOutputs,
                                      Vertex processEntityVertex) {
        if (areSame(oldProcessOutputs, newProcessOutputs)) {
            return;
        }

        removeOutputFeeds(oldProcessOutputs, processEntityVertex);
        addOutputFeeds(newProcessOutputs, processEntityVertex);
    }

    public static boolean areSame(Outputs oldProcessOutputs, Outputs newProcessOutputs) {
        if (oldProcessOutputs == null && newProcessOutputs == null
                || oldProcessOutputs == null || newProcessOutputs == null
                || oldProcessOutputs.getOutputs().size() != newProcessOutputs.getOutputs().size()) {
            return false;
        }

        List<Output> oldOutputs = oldProcessOutputs.getOutputs();
        List<Output> newOutputs = newProcessOutputs.getOutputs();

        return oldOutputs.size() == newOutputs.size()
                && oldOutputs.containsAll(newOutputs)
                && newOutputs.containsAll(oldOutputs);
    }
}
