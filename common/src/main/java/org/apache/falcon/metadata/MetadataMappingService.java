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
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.metadata.LineageRecorder.Arg;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Metadata relationship mapping service.
 */
public class MetadataMappingService implements FalconService, ConfigurationChangeListener {

    private static final Logger LOG = Logger.getLogger(MetadataMappingService.class);

    public static final String SERVICE_NAME = MetadataMappingService.class.getSimpleName();

    private KeyIndexableGraph graph;
    private EntityRelationshipGraphBuilder entityGraphBuilder;
    private InstanceRelationshipGraphBuilder instanceGraphBuilder;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        try {
            LOG.info("Initializing graph db");
            graph = initializeGraphDB();
            createIndicesForVertexKeys();

            entityGraphBuilder = new EntityRelationshipGraphBuilder(graph);
            instanceGraphBuilder = new InstanceRelationshipGraphBuilder(graph);

            ConfigurationStore.get().registerListener(this);
        } catch (URISyntaxException e) {
            throw new FalconException("Error opening graph configuration", e);
        }
    }

    protected KeyIndexableGraph getGraph() {
        return graph;
    }

    private KeyIndexableGraph initializeGraphDB() throws URISyntaxException {
        URI uri = GraphBuilder.class.getResource("/graph.properties").toURI();
        File confFile = new File(uri);
        return TitanFactory.open(confFile.toString());
    }

    private void createIndicesForVertexKeys() {
        graph.createKeyIndex(RelationshipGraphBuilder.NAME_PROPERTY_KEY, Vertex.class);
        graph.createKeyIndex(RelationshipGraphBuilder.TYPE_PROPERTY_KEY, Vertex.class);
        graph.createKeyIndex(RelationshipGraphBuilder.VERSION_PROPERTY_KEY, Vertex.class);
        graph.createKeyIndex(RelationshipGraphBuilder.TIMESTAMP_PROPERTY_KEY, Vertex.class);
    }

    @Override
    public void destroy() throws FalconException {
        graph.shutdown();
    }

    @Override
    public void onAdd(Entity entity, boolean ignoreFailure) throws FalconException {
        switch (entity.getEntityType()) {
            case CLUSTER:
                entityGraphBuilder.addClusterEntity((Cluster) entity);
                break;

            case FEED:
                entityGraphBuilder.addFeedEntity((Feed) entity);
                break;

            case PROCESS:
                entityGraphBuilder.addProcessEntity((Process) entity);
                break;

            default:
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // do nothing, we'd leave the deleted entities as-is for historical purposes
        // should we mark 'em as deleted?
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        switch (newEntity.getEntityType()) {
            case CLUSTER:
                // a cluster cannot be updated
                break;

            case FEED:
                entityGraphBuilder.updateFeedEntity((Feed) oldEntity, (Feed) newEntity);
                break;

            case PROCESS:
                entityGraphBuilder.updateProcessEntity((Process) oldEntity, (Process) newEntity);
                break;

            default:
        }
    }

    public void mapLineage(Map<String, String> message) throws FalconException {
        String nominalTime = message.get(Arg.NOMINAL_TIME.getOptionName());
        String logDir = message.get(Arg.LOG_DIR.getOptionName());

        Map<String, String> lineageMetadata = LineageRecorder.parseLineageMetadata(logDir, nominalTime);

        Vertex processInstance = instanceGraphBuilder.addProcessInstance(lineageMetadata);
        instanceGraphBuilder.addOutputFeedInstances(lineageMetadata, processInstance);
        instanceGraphBuilder.addInputFeedInstances(lineageMetadata, processInstance);
    }

    protected void debug() {
        System.out.println("--------------------------------------");
        System.out.println("Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
            System.out.println(vertexString(vertex));
        }

        System.out.println("--------------------------------------");
        System.out.println("Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(edgeString(edge));
        }
        System.out.println("--------------------------------------");
    }

    public static String vertexString(final Vertex vertex) {
        return "v[" + vertex.getId() + "], [name: "
                + vertex.getProperty("name")
                + ", type: "
                + vertex.getProperty("type")
                + "]";
    }

    public static String edgeString(final Edge edge) {
        return "e[" + edge.getLabel() + "], ["
                + edge.getVertex(Direction.OUT).getProperty("name")
                + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN).getProperty("name")
                + "]";
    }
}
