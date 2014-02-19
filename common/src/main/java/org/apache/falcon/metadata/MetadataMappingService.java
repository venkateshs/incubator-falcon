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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 *
 */
public class MetadataMappingService implements FalconService, ConfigurationChangeListener {

    private static final Logger LOG = Logger.getLogger(MetadataMappingService.class);

    public static final String SERVICE_NAME = MetadataMappingService.class.getSimpleName();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {

    }

    @Override
    public void destroy() throws FalconException {

    }

    @Override
    public void onAdd(Entity entity, boolean ignoreFailure) throws FalconException {

    }

    @Override
    public void onRemove(Entity entity) throws FalconException {

    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {

    }

    public void mapLineage(Map<String, String> lineageMetadata) {

    }
}
