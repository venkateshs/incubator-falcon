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
package org.apache.falcon.service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.falcon.messaging.EntityInstanceMessage;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.mortbay.log.Log;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.jms.*;

/**
 * Test for FalconTopicSubscriber.
 */
public class FalconTopicSubscriberTest {

    private static final String BROKER_URL = "vm://localhost";
    private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String TOPIC_NAME = "FALCON.ENTITY.TOPIC";
    private static final String SECONDARY_TOPIC_NAME = "FALCON.ENTITY.SEC.TOPIC";
    private BrokerService broker;

    @BeforeClass
    public void setup() throws Exception {
        broker = new BrokerService();
        broker.addConnector(BROKER_URL);
        broker.setDataDirectory("target/activemq");
        broker.setBrokerName("localhost");
        broker.start();
    }

    public void sendMessages(String topic) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(topic);
        MessageProducer producer = session
                .createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < 3; i++) {
            EntityInstanceMessage falconMessage = getMockFalconMessage(i);
            MapMessage message = session.createMapMessage();
            for (ARG arg : ARG.values()) {
                message.setString(arg.getPropName(), falconMessage
                        .getKeyValueMap().get(arg));
            }
            Log.debug("Sending:" + message);
            producer.send(message);
        }

        EntityInstanceMessage message = getMockFalconMessage(15);
        MapMessage mapMessage = session.createMapMessage();
        message.getKeyValueMap().put(ARG.status, "FAILED");
        for (ARG arg : ARG.values()) {
            mapMessage.setString(arg.getPropName(), message.getKeyValueMap().get(arg));
        }
        producer.send(mapMessage);
    }

    private EntityInstanceMessage getMockFalconMessage(int i) {
        EntityInstanceMessage message = new EntityInstanceMessage();
        message.getKeyValueMap().put(ARG.brokerImplClass, BROKER_IMPL_CLASS);
        message.getKeyValueMap().put(ARG.brokerUrl, BROKER_URL);
        message.getKeyValueMap().put(ARG.entityName, "process1");
        message.getKeyValueMap().put(ARG.entityType, "PROCESS");
        message.getKeyValueMap().put(ARG.feedInstancePaths,
                "/clicks/hour/00/0" + i);
        message.getKeyValueMap().put(ARG.feedNames, "clicks");
        message.getKeyValueMap().put(ARG.logFile, "/logfile");
        message.getKeyValueMap().put(ARG.nominalTime, "2012-10-10-10-10");
        message.getKeyValueMap().put(ARG.operation, "GENERATE");
        message.getKeyValueMap().put(ARG.runId, "0");
        message.getKeyValueMap().put(ARG.timeStamp, "2012-10-10-10-1" + i);
        message.getKeyValueMap().put(ARG.workflowId, "workflow-" + i);
        message.getKeyValueMap().put(ARG.topicName, TOPIC_NAME);
        message.getKeyValueMap().put(ARG.status, "SUCCEEDED");
        message.getKeyValueMap().put(ARG.workflowUser, "falcon");
        return message;
    }

    @Test
    public void testSubscriber() throws Exception{
        //Comma separated topics are supported in startup properties
        FalconTopicSubscriber subscriber = new FalconTopicSubscriber(
                BROKER_IMPL_CLASS, "", "", BROKER_URL, TOPIC_NAME+","+SECONDARY_TOPIC_NAME);
        subscriber.startSubscriber();
        sendMessages(TOPIC_NAME);
        Assert.assertEquals(broker.getAdminView().getTotalEnqueueCount(), 9);
        sendMessages(SECONDARY_TOPIC_NAME);
        Assert.assertEquals(broker.getAdminView().getTotalEnqueueCount(), 17);
        Assert.assertEquals(broker.getAdminView().getTotalConsumerCount(), 2);
        subscriber.closeSubscriber();
    }

    @AfterClass
    public void tearDown() throws Exception {
        broker.deleteAllMessages();
        broker.stop();
    }
}
