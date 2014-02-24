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
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.LocationType;

import java.net.URISyntaxException;
import java.util.Map;

/**
 * Instance Metadata relationship mapping helper.
 */
public class InstanceRelationshipGraphBuilder extends RelationshipGraphBuilder {

    // instance vertex types
    public static final String FEED_INSTANCE_TYPE = "feed-instance";
    public static final String PROCESS_INSTANCE_TYPE = "process-instance";
    public static final String WORKFLOW_INSTANCE_TYPE = "workflow-instance";

    // instance edge labels
    public static final String INSTANCE_ENTITY_EDGE_LABEL = "instance of";

    public InstanceRelationshipGraphBuilder(KeyIndexableGraph graph) {
        super(graph);
    }

    public Vertex addProcessInstance(Map<String, String> lineageMetadata) throws FalconException {
        String entityName = lineageMetadata.get(LineageRecorder.Arg.ENTITY_NAME.getOptionName());
        String processInstanceName = getProcessInstance(
                lineageMetadata.get(LineageRecorder.Arg.NOMINAL_TIME.getOptionName()));

        String timestamp = lineageMetadata.get(LineageRecorder.Arg.TIMESTAMP.getOptionName());
        Vertex processInstance = addVertex(processInstanceName, PROCESS_INSTANCE_TYPE, timestamp);

        Vertex workflowInstance = addWorkflowInstance(lineageMetadata);
        processInstance.addEdge(PROCESS_WORKFLOW_EDGE_LABEL, workflowInstance);
        addInstanceToEntity(workflowInstance, lineageMetadata.get(LineageRecorder.Arg.CLUSTER.getOptionName()),
                EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE, PROCESS_CLUSTER_EDGE_LABEL);

        addInstanceToEntity(processInstance, entityName,
                EntityRelationshipGraphBuilder.PROCESS_ENTITY_TYPE, INSTANCE_ENTITY_EDGE_LABEL);
        addInstanceToEntity(processInstance, lineageMetadata.get(LineageRecorder.Arg.CLUSTER.getOptionName()),
                EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE, PROCESS_CLUSTER_EDGE_LABEL);
        addInstanceToEntity(processInstance,
                lineageMetadata.get(LineageRecorder.Arg.WORKFLOW_USER.getOptionName()), USER_TYPE, USER_LABEL);

        org.apache.falcon.entity.v0.process.Process
                process = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
        addDataClassification(process.getTags(), processInstance);
        return processInstance;
    }

    public Vertex addWorkflowInstance(Map<String, String> lineageMetadata) {
        String workflowId = lineageMetadata.get(LineageRecorder.Arg.WORKFLOW_ID.getOptionName());
        Vertex workflowInstance = addVertex(workflowId,
                WORKFLOW_INSTANCE_TYPE, lineageMetadata.get(LineageRecorder.Arg.TIMESTAMP.getOptionName()));
        workflowInstance.setProperty(VERSION_PROPERTY_KEY, "1.0");

        String workflowName = lineageMetadata.get(LineageRecorder.Arg.USER_WORKFLOW_NAME.getOptionName());
        addInstanceToEntity(workflowInstance, workflowName,
                EntityRelationshipGraphBuilder.USER_WORKFLOW_TYPE, PROCESS_WORKFLOW_EDGE_LABEL);
        addInstanceToEntity(workflowInstance,
                lineageMetadata.get(LineageRecorder.Arg.WORKFLOW_USER.getOptionName()), USER_TYPE, USER_LABEL);

        return workflowInstance;
    }

    public String getProcessInstance(String nominalTime) throws FalconException {
        return nominalTime;
    }

    public void addInstanceToEntity(Vertex instance, String entityName,
                                     String entityType, String edgeLabel) {
        Vertex entityVertex = findVertex(entityName, entityType);
        if (entityVertex == null) {
            throw new IllegalStateException(entityType + " entity must exist " + entityName);
        }

        instance.addEdge(edgeLabel, entityVertex);
    }

    public void addOutputFeedInstances(Map<String, String> lineageMetadata,
                                        Vertex processInstance) throws FalconException {
        String[] outputFeedNames = lineageMetadata.get(LineageRecorder.Arg.FEED_NAMES.getOptionName()).split(",");
        String[] outputFeedInstancePaths =
                lineageMetadata.get(LineageRecorder.Arg.FEED_INSTANCE_PATHS.getOptionName()).split(",");

        addFeedInstances(outputFeedNames, outputFeedInstancePaths,
                processInstance, PROCESS_FEED_EDGE_LABEL, lineageMetadata);
    }

    public void addInputFeedInstances(Map<String, String> lineageMetadata,
                                       Vertex processInstance) throws FalconException {
        String[] inputFeedNames = lineageMetadata.get(LineageRecorder.Arg.INPUT_FEED_NAMES.getOptionName()).split(",");
        String[] inputFeedInstancePaths = lineageMetadata.get(LineageRecorder.Arg.INPUT_FEED_PATHS.getOptionName()).split(",");

        addFeedInstances(inputFeedNames, inputFeedInstancePaths,
                processInstance, FEED_PROCESS_EDGE_LABEL, lineageMetadata);
    }

    public void addFeedInstances(String[] feedNames, String[] feedInstancePaths,
                                  Vertex processInstance, String edgeDirection,
                                  Map<String, String> lineageMetadata) throws FalconException {
        String clusterName = lineageMetadata.get(LineageRecorder.Arg.CLUSTER.getOptionName());

        for (int index = 0; index < feedNames.length; index++) {
            String feedName = feedNames[index];
            String feedInstancePath = feedInstancePaths[index];

            String instance = getFeedInstance(feedName, clusterName, feedInstancePath);
            Vertex feedInstance = addVertex(instance, FEED_INSTANCE_TYPE,
                    lineageMetadata.get(LineageRecorder.Arg.TIMESTAMP.getOptionName()));

            addProcessFeedEdge(processInstance, feedInstance, edgeDirection);

            addInstanceToEntity(feedInstance, feedName,
                    EntityRelationshipGraphBuilder.FEED_ENTITY_TYPE, INSTANCE_ENTITY_EDGE_LABEL);
            addInstanceToEntity(feedInstance, lineageMetadata.get(LineageRecorder.Arg.CLUSTER.getOptionName()),
                    EntityRelationshipGraphBuilder.CLUSTER_ENTITY_TYPE, FEED_CLUSTER_EDGE_LABEL);
            addInstanceToEntity(feedInstance,
                    lineageMetadata.get(LineageRecorder.Arg.WORKFLOW_USER.getOptionName()), USER_TYPE, USER_LABEL);

            Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
            addDataClassification(feed.getTags(), feedInstance);
            addGroups(feed.getGroups(), feedInstance);
        }
    }

    public String getFeedInstance(String feedName, String clusterName,
                                  String feedInstancePath) throws FalconException {
        try {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
            Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);

            Storage.TYPE storageType = FeedHelper.getStorageType(feed, cluster);
            return storageType == Storage.TYPE.TABLE
                    ? getTableFeedInstance(feedInstancePath, storageType)
                    : getFileSystemFeedInstance(feedInstancePath, feed, cluster);

        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }
    }

    private String getTableFeedInstance(String feedInstancePath,
                                        Storage.TYPE storageType) throws URISyntaxException {
        CatalogStorage instanceStorage = (CatalogStorage) FeedHelper.createStorage(
                storageType.name(), feedInstancePath);
        return instanceStorage.toPartitionAsPath();
    }

    private String getFileSystemFeedInstance(String feedInstancePath, Feed feed,
                                             Cluster cluster) throws FalconException {
        Storage rawStorage = FeedHelper.createStorage(cluster, feed);
        String feedPathTemplate = rawStorage.getUriTemplate(LocationType.DATA);
        String instance = feedInstancePath;
        String[] elements = FeedDataPath.PATTERN.split(feedPathTemplate);
        for (String element : elements) {
            System.out.println("element = " + element);
            instance = instance.replaceFirst(element, "");
            System.out.println("instance = " + instance);
        }
        return instance;
    }
}
