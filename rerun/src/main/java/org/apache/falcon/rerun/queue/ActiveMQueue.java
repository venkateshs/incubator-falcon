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
package org.apache.falcon.rerun.queue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.falcon.FalconException;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.rerun.event.RerunEventFactory;

import javax.jms.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An ActiveMQ implementation for DelayedQueue.
 *
 * @param <T>
 */
public class ActiveMQueue<T extends RerunEvent> extends DelayedQueue<T> {

    private ActiveMQConnection connection;
    private String brokerUrl;
    private String destinationName;
    private Destination destination;
    private MessageProducer producer;
    private MessageConsumer consumer;

    public ActiveMQueue(String brokerUrl, String destinationName) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
    }

    @Override
    public boolean offer(T event) throws FalconException {
        Session session;
        try {
            session = getSession();
            TextMessage msg = session.createTextMessage(event.toString());
            msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY,
                    event.getDelay(TimeUnit.MILLISECONDS));
            msg.setStringProperty("TYPE", event.getType().name());
            producer.send(msg);
            LOG.debug("Enqueued Message:" + event.toString() + "with delay "
                    + event.getDelay(TimeUnit.MILLISECONDS) + " milli sec");
            return true;
        } catch (Exception e) {
            LOG.error("Unable to offer event:" + event + " to activeMqueue", e);
            throw new FalconException("Unable to offer event:" + event + " to activeMqueue", e);
        }
    }

    private Session getSession() throws Exception {
        if (connection == null) {
            init();
        }

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session;
    }

    @Override
    public T take() throws FalconException {
        try {
            TextMessage textMessage = (TextMessage) consumer.receive();
            T event = new RerunEventFactory<T>().getRerunEvent(
                    textMessage.getStringProperty("TYPE"),
                    textMessage.getText());
            LOG.debug("Dequeued Message:" + event.toString());
            return event;
        } catch (Exception e) {
            LOG.error("Error getting the messge from ActiveMqueue: ", e);
            throw new FalconException("Error getting the messge from ActiveMqueue: ", e);
        }
    }

    @Override
    public void populateQueue(List<T> events) {
    }

    @Override
    public void init() {
        try {
            createAndStartConnection("", "", brokerUrl);
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(destinationName);
            producer = session.createProducer(destination);
            consumer = session.createConsumer(destination);
            LOG.info("Initialized Queue on activeMQ: " + destinationName);
        } catch (Exception e) {
            LOG.error(
                    "Error starting ActiveMQueue connection for dealyed queue",
                    e);
            throw new RuntimeException(
                    "Error starting ActiveMQueue connection for delayed queue",
                    e);
        }
    }

    private void createAndStartConnection(String userName, String password,
                                          String url) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                userName, password, url);
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        LOG.info("Connected successfully to " + url);
    }

    @Override
    public void reconnect() throws FalconException {
        try {
            LOG.info("Attempting to close producer");
            producer.close();
            LOG.info("Producer closed successfully");
        } catch (Exception ignore) {
            LOG.info("Producer could not be closed");
        }

        try {
            LOG.info("Attempting to close consumer");
            consumer.close();
            LOG.info("Consumer closed successfully");
        } catch (Exception ignore) {
            LOG.info("Consumer could not be closed");
        }

        try {
            LOG.info("Attempting to close connection");
            connection.close();
            LOG.info("Connection closed successfully");
        } catch (Exception ignore) {
            LOG.info("Connection could not be closed");
        }

        init();
    }
}
