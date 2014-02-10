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

package org.apache.falcon.resource;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.testng.Assert;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base test class for CLI, Entity and Process Instances.
 */
public class TestContext {
    public static final String FEED_TEMPLATE1 = "/feed-template1.xml";
    public static final String FEED_TEMPLATE2 = "/feed-template2.xml";

    public static final String CLUSTER_TEMPLATE = "/cluster-template.xml";

    public static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";
    public static final String PROCESS_TEMPLATE = "/process-template.xml";
    public static final String PIG_PROCESS_TEMPLATE = "/pig-process-template.xml";

    public static final String BASE_URL = "http://localhost:41000/falcon-webapp";
    public static final String REMOTE_USER = FalconClient.USER;

    private static final String AUTH_COOKIE_EQ = AuthenticatedURL.AUTH_COOKIE + "=";

    protected Unmarshaller unmarshaller;
    protected Marshaller marshaller;

    protected EmbeddedCluster cluster;
    protected WebResource service = null;
    private AuthenticatedURL.Token authenticationToken;

    protected String clusterName;
    protected String processName;
    protected String outputFeedName;

    public static final Pattern VAR_PATTERN = Pattern.compile("##[A-Za-z0-9_]*##");

    public TestContext() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(APIResult.class, Feed.class, Process.class, Cluster.class,
                    InstancesResult.class);
            unmarshaller = jaxbContext.createUnmarshaller();
            marshaller = jaxbContext.createMarshaller();
            configure();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void configure() throws Exception {
        StartupProperties.get().setProperty(
                "application.services",
                StartupProperties.get().getProperty("application.services")
                        .replace("org.apache.falcon.service.ProcessSubscriberService", ""));
        String store = StartupProperties.get().getProperty("config.store.uri");
        StartupProperties.get().setProperty("config.store.uri", store + System.currentTimeMillis());

        try {
            String baseUrl = BASE_URL;
            if (!baseUrl.endsWith("/")) {
                baseUrl += "/";
            }
            this.authenticationToken = FalconClient.getToken(baseUrl);
        } catch (FalconCLIException e) {
            throw new AuthenticationException(e);
        }

        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.setReadTimeout(500000);
        client.setConnectTimeout(500000);
        this.service = client.resource(UriBuilder.fromUri(BASE_URL).build());
    }

    public void setCluster(String cName) throws Exception {
        cluster = EmbeddedCluster.newCluster(cName, true);
        this.clusterName = cluster.getCluster().getName();
    }

    public EmbeddedCluster getCluster() {
        return cluster;
    }

    public WebResource getService() {
        return service;
    }

    public String getAuthenticationToken() {
        return AUTH_COOKIE_EQ + authenticationToken;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getProcessName() {
        return processName;
    }

    public String getClusterFileTemplate() {
        return CLUSTER_TEMPLATE;
    }

    public void scheduleProcess(String processTemplate, Map<String, String> overlay) throws Exception {
        ClientResponse response = submitToFalcon(CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(processTemplate, overlay, EntityType.PROCESS);
        assertSuccessful(response);

        ClientResponse clientRepsonse = this.service
                .path("api/entities/schedule/process/" + processName)
                .header("Cookie", getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertSuccessful(clientRepsonse);
    }

    public void scheduleProcess() throws Exception {
        scheduleProcess(PROCESS_TEMPLATE, getUniqueOverlay());
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param fileName file name
     * @return ServletInputStream
     * @throws java.io.IOException
     */
    public ServletInputStream getServletInputStream(String fileName) throws IOException {
        return getServletInputStream(new FileInputStream(fileName));
    }

    public ServletInputStream getServletInputStream(final InputStream stream) {
        return new ServletInputStream() {

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }

    public ClientResponse submitAndSchedule(String template, Map<String, String> overlay, EntityType entityType)
        throws Exception {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        return this.service.path("api/entities/submitAndSchedule/" + entityType.name().toLowerCase())
                .header("Cookie", getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .type(MediaType.TEXT_XML)
                .post(ClientResponse.class, rawlogStream);
    }

    public ClientResponse submitToFalcon(String template, Map<String, String> overlay, EntityType entityType)
        throws IOException {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        if (entityType == EntityType.CLUSTER) {
            try {
                cluster = EmbeddedCluster.newCluster(overlay.get("cluster"), true);
                clusterName = cluster.getCluster().getName();
            } catch (Exception e) {
                throw new IOException("Unable to setup cluster info", e);
            }
        }
        return submitFileToFalcon(entityType, tmpFile);
    }

    public ClientResponse submitFileToFalcon(EntityType entityType, String tmpFile) throws IOException {

        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        return this.service.path("api/entities/submit/" + entityType.name().toLowerCase())
                .header("Cookie", getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .type(MediaType.TEXT_XML)
                .post(ClientResponse.class, rawlogStream);
    }

    public void assertStatus(ClientResponse clientResponse, APIResult.Status status) {
        String response = clientResponse.getEntity(String.class);
        try {
            APIResult result = (APIResult) unmarshaller.unmarshal(new StringReader(response));
            Assert.assertEquals(result.getStatus(), status);
        } catch (JAXBException e) {
            Assert.fail("Response " + response + " is not valid");
        }
    }

    public void assertFailure(ClientResponse clientResponse) {
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertStatus(clientResponse, APIResult.Status.FAILED);
    }

    public void assertSuccessful(ClientResponse clientResponse) {
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        assertStatus(clientResponse, APIResult.Status.SUCCEEDED);
    }

    public static String overlayParametersOverTemplate(String template,
                                                       Map<String, String> overlay) throws IOException {
        File tmpFile = getTempFile();
        OutputStream out = new FileOutputStream(tmpFile);

        InputStreamReader in;
        InputStream resourceAsStream = TestContext.class.getResourceAsStream(template);
        if (resourceAsStream == null) {
            in = new FileReader(template);
        } else {
            in = new InputStreamReader(resourceAsStream);
        }
        BufferedReader reader = new BufferedReader(in);
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = VAR_PATTERN.matcher(line);
            while (matcher.find()) {
                String variable = line.substring(matcher.start(), matcher.end());
                line = line.replace(variable, overlay.get(variable.substring(2, variable.length() - 2)));
                matcher = VAR_PATTERN.matcher(line);
            }
            out.write(line.getBytes());
            out.write("\n".getBytes());
        }
        reader.close();
        out.close();
        return tmpFile.getAbsolutePath();
    }

    public static File getTempFile() throws IOException {
        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
        }

        return File.createTempFile("test", ".xml", target);
    }

    public Map<String, String> getUniqueOverlay() throws FalconException {
        Map<String, String> overlay = new HashMap<String, String>();
        long time = System.currentTimeMillis();
        clusterName = "cluster" + time;
        overlay.put("cluster", clusterName);
        overlay.put("colo", "gs");
        overlay.put("inputFeedName", "in" + time);
        //only feeds with future dates can be scheduled
        Date endDate = new Date(System.currentTimeMillis() + 15 * 60 * 1000);
        overlay.put("feedEndDate", SchemaHelper.formatDateUTC(endDate));
        overlay.put("outputFeedName", "out" + time);
        processName = "p" + time;
        overlay.put("processName", processName);
        outputFeedName = "out" + time;
        return overlay;
    }

    public static void prepare() throws Exception {
        prepare(TestContext.CLUSTER_TEMPLATE);
    }

    private static void mkdir(FileSystem fs, Path path) throws Exception {
        if (!fs.exists(path) && !fs.mkdirs(path)) {
            throw new Exception("mkdir failed for " + path);
        }
    }

    private static void mkdir(FileSystem fs, Path path, FsPermission perm) throws Exception {
        if (!fs.exists(path) && !fs.mkdirs(path, perm)) {
            throw new Exception("mkdir failed for " + path);
        }
    }

    public static void prepare(String clusterTemplate) throws Exception {
        // setup a logged in user
        CurrentUser.authenticate(REMOTE_USER);

        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("cluster", RandomStringUtils.randomAlphabetic(5));
        overlay.put("colo", "gs");
        TestContext.overlayParametersOverTemplate(clusterTemplate, overlay);
        EmbeddedCluster cluster = EmbeddedCluster.newCluster(overlay.get("cluster"), true);

        cleanupStore();

        // setup dependent workflow and lipath in hdfs
        FileSystem fs = FileSystem.get(cluster.getConf());
        mkdir(fs, new Path("/falcon"), new FsPermission((short) 511));

        Path wfParent = new Path("/falcon/test");
        fs.delete(wfParent, true);
        Path wfPath = new Path(wfParent, "workflow");
        mkdir(fs, wfPath);
        fs.copyFromLocalFile(false, true,
                new Path(TestContext.class.getResource("/fs-workflow.xml").getPath()),
                new Path(wfPath, "workflow.xml"));
        mkdir(fs, new Path(wfParent, "input/2012/04/20/00"));
        Path outPath = new Path(wfParent, "output");
        mkdir(fs, outPath, new FsPermission((short) 511));
    }

    public static void cleanupStore() throws Exception {
        for (EntityType type : EntityType.values()) {
            for (String name : ConfigurationStore.get().getEntities(type)) {
                ConfigurationStore.get().remove(type, name);
            }
        }
    }

    public static int executeWithURL(String command) throws Exception {
        return new FalconCLI().run((command + " -url " + TestContext.BASE_URL).split("\\s+"));
    }
}
