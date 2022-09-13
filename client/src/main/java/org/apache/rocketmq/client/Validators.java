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

package org.apache.rocketmq.client;

import static org.apache.rocketmq.common.topic.TopicValidator.isTopicOrGroupIllegal;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.topic.TopicValidator;

/**
 * Common Validator
 */
public class Validators {
    public static final int CHARACTER_MAX_LENGTH = 255;
    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * Validate group
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }

        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }


        if (isTopicOrGroupIllegal(group)) {
            throw new MQClientException(String.format(
                    "the specified group[%s] contains illegal characters, allowing only %s", group,
                    "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    /**
     * Validators的方法
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
        //如果消息为null，抛出异常
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        /*
         * 校验topic
         */
        //如果topic为空，或者长度大于127个字符，或者topic的字符串不符合 "^[%|a-zA-Z0-9_-]+$"模式，即包含非法字符，那么抛出异常
        Validators.checkTopic(msg.getTopic());
        //如果当前topic是不为允许使用的系统topic，那么抛出异常
        Validators.isNotAllowedSendTopic(msg.getTopic());

        // body
        //如果消息体为null，那么抛出异常
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }
        //如果消息体为空数组，那么抛出异常
        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }
        //如果消息 字节数组长度大于4,194,304，即消息的大小大于4M，那么抛出异常
        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL,
                    "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
    }

    public static void checkTopic(String topic) throws MQClientException {
        //如果topic为空，那么抛出异常
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }
        //如果topic长度大于127个字符，那么抛出异常
        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(
                    String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }
        //如果topic字符串包含非法字符，那么抛出异常
        if (isTopicOrGroupIllegal(topic)) {
            throw new MQClientException(String.format(
                    "The specified topic[%s] contains illegal characters, allowing only %s", topic,
                    "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    public static void isSystemTopic(String topic) throws MQClientException {
        if (TopicValidator.isSystemTopic(topic)) {
            throw new MQClientException(
                    String.format("The topic[%s] is conflict with system topic.", topic), null);
        }
    }

    public static void isNotAllowedSendTopic(String topic) throws MQClientException {
        if (TopicValidator.isNotAllowedSendTopic(topic)) {
            throw new MQClientException(
                    String.format("Sending message to topic[%s] is forbidden.", topic), null);
        }
    }

}
