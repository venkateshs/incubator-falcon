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

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.*;

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
            addCluster(feedCluster.getName(), feedVertex, FEED_CLUSTER_EDGE_LABEL);
        }
    }

    public void addCluster(String clusterName, Vertex fromVertex,
                            String edgeLabel) {
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
            addCluster(cluster.getName(), processVertex, PROCESS_CLUSTER_EDGE_LABEL);
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
        Vertex workflowVertex = addVertex(workflow.getName(), USER_WORKFLOW_TYPE, System.currentTimeMillis());
        workflowVertex.setProperty(VERSION_PROPERTY_KEY, workflow.getVersion());
        workflowVertex.setProperty("engine", workflow.getEngine().value());

        processVertex.addEdge(PROCESS_WORKFLOW_EDGE_LABEL, workflowVertex);

        addUser(workflowVertex); // is this necessary?
    }
}
