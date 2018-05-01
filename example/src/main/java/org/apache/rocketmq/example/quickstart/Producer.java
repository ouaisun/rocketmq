/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        final DefaultMQProducer producer = new DefaultMQProducer("Producer");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Launch the instance.
         */
        producer.setNamesrvAddr("10.95.116.57:9876");
        producer.start();
        Thread.sleep(1000);
        ExecutorService      executorService = Executors.newFixedThreadPool(10);
        final CountDownLatch countDownLatch  = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            final int index = i;

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        /*
                         * Create a message instance, specifying topic, tag and message body.
                         */
                        countDownLatch.countDown();
                        Message msg = new Message("TopicTest" /* Topic */,
                                "TagA" /* Tag */,
                                ("Hello RocketMQ " + index).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                        );

                        /*
                         * Call send message to deliver message to one of brokers.
                         */
                        SendResult sendResult = producer.send(msg);

                        System.out.printf("%d -> %s%n", countDownLatch.getCount(), sendResult);
                    } catch (Exception e) {
                        e.printStackTrace();
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            });
        }
        countDownLatch.await();
        System.out.println("out");
        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
