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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class LineageRecorder  extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(LineageRecorder.class);

    /**
     * Args that the utility understands.
     */
    public enum Arg {
        // process instance
        NOMINAL_TIME("nominalTime", "instance time"),
        ENTITY_TYPE("entityType", "type of the entity"),
        ENTITY_NAME("entityName", "name of the entity"),
        TIMESTAMP("timeStamp", "current timestamp"),

        // where
        CLUSTER("cluster", "name of the current cluster"),
        OPERATION("operation", "operation like generate, delete, replicate"),

        // who
        WORKFLOW_USER("workflowUser", "user who owns the feed instance (partition)"),

        // workflow details
        WORKFLOW_ID("workflowId", "current workflow-id of the instance"),
        RUN_ID("runId", "current run-id of the instance"),
        STATUS("status", "status of the user workflow isnstance"),
        WF_ENGINE_URL("workflowEngineUrl", "url of workflow engine server, ex:oozie"),
        USER_SUBFLOW_ID("subflowId", "external id of user workflow"),
        USER_WORKFLOW_ENGINE("userWorkflowEngine", "user workflow engine type"),
        USER_WORKFLOW_NAME("userWorkflowName", "user workflow name"),
        USER_WORKFLOW_VERSION("userWorkflowVersion", "user workflow version"),

        // what inputs
        INPUT_FEED_NAMES("falconInputFeeds", "name of the feeds which are used as inputs"),
        INPUT_FEED_PATHS("falconInputPaths", "comma separated input feed instance paths"),
        INPUT_FEED_TYPES("falconInputTypes", "comma separated input feed storage types"),

        // what outputs
        FEED_NAMES("feedNames", "name of the feeds which are generated/replicated/deleted"),
        FEED_INSTANCE_PATHS("feedInstancePaths", "comma separated feed instance paths"),

        // misc
        LOG_DIR("logDir", "log dir where lineage can be recorded"),
        // LINEAGE_FILE("lineageFile", "File where lineage will be recorded"),
        ;

        private String name;
        private String description;

        Arg(String name, String description) {
            this.name = name;
            this.description = description;
        }

        public Option getOption() {
            return new Option(this.name, true, this.description);
        }

        public String getOptionName() {
            return this.name;
        }

        public String getOptionValue(CommandLine cmd) {
            return cmd.getOptionValue(this.name);
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new LineageRecorder(), args);
    }

    @Override
    public int run(String[] arguments) throws Exception {
        CommandLine command = getCommand(arguments);

        Map<String, String> lineageMetadata = getLineageMetadata(command);
        LOG.info("Lineage Metadata: " + lineageMetadata);

        String lineageFile = getFilePath(command.getOptionValue("logDir"),
                command.getOptionValue("nominalTime"));
        persistLineageMetadata(lineageMetadata, lineageFile);

        return 0;
    }

    protected static CommandLine getCommand(String[] arguments) throws ParseException {

        Options options = new Options();

        for (Arg arg : Arg.values()) {
            addOption(options, arg);
        }

        return new GnuParser().parse(options, arguments);
    }

    private static void addOption(Options options, Arg arg) {
        addOption(options, arg, true);
    }

    private static void addOption(Options options, Arg arg, boolean isRequired) {
        Option option = arg.getOption();
        option.setRequired(isRequired);
        options.addOption(option);
    }

    protected Map<String, String> getLineageMetadata(CommandLine command) {
        Map<String, String> lineageMetadata = new HashMap<String, String>();

        for (Arg arg : Arg.values()) {
            lineageMetadata.put(arg.getOptionName(), arg.getOptionValue(command));
        }

        return lineageMetadata;
    }

    public static String getFilePath(String logDir, String nominalTime) {
        return logDir + "/lineage/" + nominalTime + ".json";
    }

    protected void persistLineageMetadata(Map<String, String> lineageMetadata,
                                        String lineageFile) throws IOException, FalconException {
        OutputStream out = null;
        Path file = new Path(lineageFile);
        try {
            FileSystem fs = HadoopClientFactory.get().createFileSystem(file.toUri(), getConf());
            out = fs.create(file);

            // making sure falcon can read this file
            FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fs.setPermission(file, permission);

            out.write(JSONValue.toJSONString(lineageMetadata).getBytes());
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ignore) {
                    // ignore
                }
            }
        }
    }

    public static Map<String, String> parseLineageMetadata(String logDir, String nominalTime)
        throws FalconException, IOException {

        String file = getFilePath(logDir, nominalTime);
        Path lineageDataPath = new Path(file); // file has 777 permissions
        FileSystem fs = HadoopClientFactory.get().createFileSystem(lineageDataPath.toUri());

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(lineageDataPath)));
        return (Map<String, String>) JSONValue.parse(in);
    }
}
