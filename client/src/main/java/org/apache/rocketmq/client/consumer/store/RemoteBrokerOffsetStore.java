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
package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Remote storage implementation
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String groupName;
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    /**
     * RemoteBrokerOffsetStore
     */
    @Override
    public void load() {
    }

    /**
     * RemoteBrokerOffsetStore的方法
     * 更新内存中的offset
     *
     * @param mq           消息队列
     * @param offset       偏移量
     * @param increaseOnly 是否仅单调增加offset，顺序消费为false，并发消费为true
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            //获取已存在的offset
            AtomicLong offsetOld = this.offsetTable.get(mq);
            //如果没有老的offset，那么将新的offset存进去
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }
            //如果有老的offset，那么尝试更新offset
            if (null != offsetOld) {
                //如果仅单调增加offset，顺序消费为false，并发消费为true
                if (increaseOnly) {
                    //如果新的offset大于已存在offset，则尝试在循环中CAS的更新为新offset
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    //直接设置为新offset，可能导致offset变小
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     * RemoteBrokerOffsetStore的方法
     * <p>
     * 获取offset
     *
     * @param mq   需要获取offset的mq
     * @param type 读取类型
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                /*
                 * 先从本地内存offsetTable读取，读不到再从broker中读取
                 */
                case MEMORY_FIRST_THEN_STORE:
                    /*
                     * 仅从本地内存offsetTable读取
                     */
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        //如果本地内存有关于此mq的offset，那么直接返回
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        //如果本地内存没有关于此mq的offset，但那读取类型为READ_FROM_MEMORY，那么直接返回-1
                        return -1;
                    }
                }
                /*
                 * 仅从broker中读取
                 */
                case READ_FROM_STORE: {
                    try {
                        /*
                         * 从broker中获取此消费者组的offset
                         */
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        //更新此mq的offset，并且存入本地offsetTable缓存
                        AtomicLong offset = new AtomicLong(brokerOffset);
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        //broker中没有关于此消费者组的offset，返回-1
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    /**
     * RemoteBrokerOffsetStore的方法
     * <p>
     * 持久化所有mq的offset到远程broker
     *
     * @param mqs 所有的mq
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;
        //未上报的mq集合
        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();
        //偏移量集合
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            AtomicLong offset = entry.getValue();
            if (offset != null) {
                //mq集合中包含该mq
                if (mqs.contains(mq)) {
                    try {
                        /*
                         * 上报消费位点到Broker
                         */
                        this.updateConsumeOffsetToBroker(mq, offset.get());
                        log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                offset.get());
                    } catch (Exception e) {
                        log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                    }
                } else {
                    //没有持久化的mq加入到未上报的mq集合中
                    unusedMQ.add(mq);
                }
            }
        }
        //对于未上报的mq，从offsetTable中进行移除
        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                        this.groupName,
                        this.mQClientFactory.getClientId(),
                        mq,
                        offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                    offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * RemoteBrokerOffsetStore的方法
     * <p>
     * 更新消费偏移量
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        //调用另一个updateConsumeOffsetToBroker方法
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * RemoteBrokerOffsetStore的方法
     * <p>
     * 更新消费偏移量
     *
     * @param mq       消息队列
     * @param offset   偏移量
     * @param isOneway 是否是单向请求，自动提交offset请求为true
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        //获取指定brokerName的master地址。
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            //从nameServer拉取并更新topic的路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            //获取指定brokerName的master地址
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, false);
        }

        if (findBrokerResult != null) {
            //构建请求头
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);
            //是否是单向请求，自动提交offset请求为true，发送七个球即返回，不管最终是否持久化成功
            if (isOneway) {
                //发送更新offset的单向请求
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                        findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                        findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    /**
     * RemoteBrokerOffsetStore
     * <p>
     * 从broker中获取此消费者组的offset
     *
     * @param mq 需要获取offset的mq
     */
    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        //获取指定brokerName的master地址
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            //从nameServer拉取并更新topic的路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            //获取指定brokerName的master地址
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, false);
        }

        if (findBrokerResult != null) {
            //构建请求头，包括topic、groupName、queueId
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            //向broker发起同步请求获取指定topic的groupName的指定队列的最新偏移量，Code为QUERY_CONSUMER_OFFSET
            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
