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

import org.apache.commons.cli.CommandLine;
import org.apache.falcon.metadata.LineageRecorder.Arg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

public class LineageRecorderTest {

    private static final String LOGS_DIR = "target/log";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";


    private String[] args;

    @BeforeMethod
    public void setUp() throws Exception {
        args = new String[]{
            "-" + Arg.ENTITY_NAME.getOptionName(), "test-process-entity",
            "-" + Arg.FEED_NAMES.getOptionName(), "out-click-logs,out-raw-logs",
            "-" + Arg.FEED_INSTANCE_PATHS.getOptionName(),
                "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20",
            "-" + Arg.WORKFLOW_ID.getOptionName(), "workflow-01-00",
            "-" + Arg.WORKFLOW_USER.getOptionName(), "falcon",
            "-" + Arg.RUN_ID.getOptionName(), "1",
            "-" + Arg.NOMINAL_TIME.getOptionName(), NOMINAL_TIME,
            "-" + Arg.TIMESTAMP.getOptionName(), "2014-01-01-01-00",
            "-" + Arg.ENTITY_TYPE.getOptionName(), ("process"),
            "-" + Arg.OPERATION.getOptionName(), ("GENERATE"),
            "-" + Arg.STATUS.getOptionName(), ("SUCCEEDED"),
            "-" + Arg.CLUSTER.getOptionName(), "corp",
            "-" + Arg.WF_ENGINE_URL.getOptionName(), "http://localhost:11000/oozie/",
            "-" + Arg.LOG_DIR.getOptionName(), LOGS_DIR,
            "-" + Arg.USER_SUBFLOW_ID.getOptionName(), "userflow@wf-id" + "test",
            "-" + Arg.USER_WORKFLOW_ENGINE.getOptionName(), "oozie",
            "-" + Arg.INPUT_FEED_NAMES.getOptionName(), "in-click-logs,in-raw-logs",
            "-" + Arg.INPUT_FEED_PATHS.getOptionName(),
                "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20",
            "-" + Arg.INPUT_FEED_TYPES.getOptionName(), "FILESYSTEM,FILESYSTEM",
            "-" + Arg.USER_WORKFLOW_NAME.getOptionName(), "test-workflow",
            "-" + Arg.USER_WORKFLOW_VERSION.getOptionName(), "1.0.0",
        };
    }

    @AfterMethod
    public void tearDown() throws Exception {

    }

    @Test
    public void testMain() throws Exception {
        LineageRecorder lineageRecorder = new LineageRecorder();
        CommandLine command = LineageRecorder.getCommand(args);
        Map<String, String> lineageMetadata = lineageRecorder.getLineageMetadata(command);

        LineageRecorder.main(args);

        String file = LineageRecorder.getFilePath(LOGS_DIR, NOMINAL_TIME);
        Path lineageDataPath = new Path(file);
        FileSystem fs = lineageDataPath.getFileSystem(new Configuration());
        Assert.assertTrue(fs.exists(lineageDataPath));

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(lineageDataPath)));
        Map<String, String> recordedLineageMetadata = (Map<String, String>) JSONValue.parse(in);

        for (Map.Entry<String, String> entry : lineageMetadata.entrySet()) {
            Assert.assertEquals(lineageMetadata.get(entry.getKey()),
                    recordedLineageMetadata.get(entry.getKey()));
        }
    }

    @Test
    public void testGetFilePath() throws Exception {
        String path = LOGS_DIR + "/lineage/" + NOMINAL_TIME + ".json";
        Assert.assertEquals(path, LineageRecorder.getFilePath(LOGS_DIR, NOMINAL_TIME));
    }
}
