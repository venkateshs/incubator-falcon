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

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.security.CurrentUser;

import java.util.Iterator;

/**
 * Base class for Metadata relationship mapping helper.
 */
public abstract class RelationshipGraphBuilder {

    // vertex property keys
    public static final String NAME_PROPERTY_KEY = "name";
    public static final String TYPE_PROPERTY_KEY = "type";
    public static final String TIMESTAMP_PROPERTY_KEY = "timestamp";
    public static final String VERSION_PROPERTY_KEY = "version";

    // vertex types
    public static final String USER_TYPE = "user";
    public static final String COLO_TYPE = "data-center";
    public static final String TAGS_TYPE = "classification";
    public static final String GROUPS_TYPE = "group";

    // edge labels
    public static final String USER_LABEL = "owned by";
    public static final String CLUSTER_COLO_LABEL = "collocated";
    public static final String GROUPS_LABEL = "grouped as";

    // entity edge labels
    public static final String FEED_CLUSTER_EDGE_LABEL = "stored in";
    public static final String PROCESS_CLUSTER_EDGE_LABEL = "runs on";
    public static final String FEED_PROCESS_EDGE_LABEL = "input";
    public static final String PROCESS_FEED_EDGE_LABEL = "output";
    public static final String PROCESS_WORKFLOW_EDGE_LABEL = "executes";

    /**
     * A blueprints graph.
     */
    private final KeyIndexableGraph graph;

    protected RelationshipGraphBuilder(KeyIndexableGraph graph) {
        this.graph = graph;
    }

    protected KeyIndexableGraph getGraph() {
        return graph;
    }


    public void addUser(Vertex fromVertex) {
        Vertex userVertex = addVertex(CurrentUser.getUser(), USER_TYPE);
        fromVertex.addEdge(USER_LABEL, userVertex);
    }

    public void addDataClassification(String classification, Vertex entityVertex) {
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

    public void addGroups(String groups, Vertex entityVertex) {
        if (groups == null || groups.length() == 0) {
            return;
        }

        String[] groupTags = groups.split(",");
        for (String groupTag : groupTags) {
            Vertex groupVertex = addVertex(groupTag, GROUPS_TYPE);
            entityVertex.addEdge(GROUPS_LABEL, groupVertex);
        }
    }

    public void addProcessFeedEdge(Vertex processVertex, Vertex feedVertex, String edgeLabel) {
        /*
        Edge edge = label.equals(FEED_PROCESS_EDGE_LABEL)
                ? feedInstance.addEdge(FEED_PROCESS_EDGE_LABEL, processInstance)
                : processInstance.addEdge(PROCESS_FEED_EDGE_LABEL, feedInstance);
        // System.out.println("edge = " + edge);
        */

        /*
        Edge edge = (edgeLabel.equals(FEED_PROCESS_EDGE_LABEL))
                ? feedVertex.addEdge(edgeLabel, processVertex)
                : processVertex.addEdge(edgeLabel, feedVertex);
        System.out.println("edge = " + edge);
        */

        if (edgeLabel.equals(FEED_PROCESS_EDGE_LABEL)) {
            feedVertex.addEdge(edgeLabel, processVertex);
        } else {
            processVertex.addEdge(edgeLabel, feedVertex);
        }
    }

    public Vertex addVertex(String name, String type) {
        Vertex vertex = findVertex(name, type);
        if (vertex == null) {
            vertex = buildVertex(name, type);
        }

        return vertex;
    }

    public Vertex addVertex(String name, String type, Object timestamp) {
        Vertex vertex = addVertex(name, type);
        vertex.setProperty(TIMESTAMP_PROPERTY_KEY, timestamp);

        return vertex;
    }

    public Vertex findVertex(String name, String type) {
        GraphQuery query = graph.query()
                .has(NAME_PROPERTY_KEY, name)
                .has(TYPE_PROPERTY_KEY, type);
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;
    }

    public Vertex buildVertex(String name, String type) {
        Vertex vertex = graph.addVertex(null);
        vertex.setProperty(NAME_PROPERTY_KEY, name);
        vertex.setProperty(TYPE_PROPERTY_KEY, type);

        return vertex;
    }
}
