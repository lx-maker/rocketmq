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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * @param consumerGroup 当前consumerGroup
     * @param currentCID    当前currentCID
     * @param mqAll         当前topic的mq，已排序
     * @param cidAll        当前consumerGroup的clientId集合，已排序
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {
        //参数校验
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                    consumerGroup,
                    currentCID,
                    cidAll);
            return result;
        }
        //当前currentCID在集合中的索引位置
        int index = cidAll.indexOf(currentCID);
        //计算平均分配后的余数，大于0表示不能被整除，必然有些消费者会多分配一个队列，有些消费者少分配一个队列
        int mod = mqAll.size() % cidAll.size();
        //计算当前消费者分配的队列数量
        //1、如果队列数量小于等于消费者数量，那么每个消费者最多只能分到一个队列，则算作1（后续还会计算），否则，表示每个消费者至少分配一个队列，需要继续计算
        //2、如果mod大于0并且当前消费者索引小于mod，那么当前消费者分到的队列数为平均分配的队列数+1，否则，分到的队列数为平均分配的队列数，即索引在余数范围内的，多分配一个队列
        int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());
        //如果mod大于0并且当前消费者索引小于mod，那么起始索引为index * averageSize，否则起始索引为index * averageSize + mod
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //最终分配的消息队列数量。取最小值是因为有些队列将会分配至较少的队列甚至无法分配到队列
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        //分配队列，按照顺序分配
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
