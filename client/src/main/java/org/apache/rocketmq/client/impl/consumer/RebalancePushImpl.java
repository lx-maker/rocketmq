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
package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
                             AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                             MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    /**
     * RebalancePushImpl的方法
     * <p>
     * 设置新的本地订阅关系版本，重设流控参数，立即给所有broker发送心跳，让Broker更新当前订阅关系
     *
     * @param topic     topic
     * @param mqAll     所有的消息队列
     * @param mqDivided 分配的消息队列
     */
    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        //获取订阅关系
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        //设置新的版本
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);
        //获取处理队列数量
        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            //topic级别的流量控制阈值，即当前consumer对于Topic在本地最大能缓存的消息数，默认-1，无限制。如果不等于-1，则该值将会被重新计算
            //例如，如果pullThresholdForTopic的值是1000，并且为该消费者分配了10个消息队列，那么pullThresholdForQueue将被设置为100
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            //如果不等于-1
            if (pullThresholdForTopic != -1) {
                //取值为 pullThresholdForTopic / currentQueueCount
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                //重设pullThresholdForTopic
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }
            //topic级别的消息缓存大小阈值，即当前consumer对于Topic在本地最大能缓存的消息大小，默认-1，无限制
            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            //如果不等于-1
            if (pullThresholdSizeForTopic != -1) {
                //取值为 pullThresholdSizeForTopic / currentQueueCount
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                //重设pullThresholdSizeForTopic
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        //主动发送心跳信息给所有broker。
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }

    /**
     * RebalancePushImpl的方法
     * <p>
     * 删除不必要的消息队列
     *
     * @param mq 需要删除的消息队列
     * @param pq 需要删除的消息队列的处理队列
     * @return 是否移除成功
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        /*
         * 保存指定消息队列的偏移量，可能在本地存储或远程服务器
         */
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        /*
         * 移除OffsetStore内部的offsetTable中的对应消息队列的k-v数据
         */
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        /*
         * Push模式下，如果当前消费者是有序消费，且是集群消费，那么尝试从Broker端将该消息队列解锁，如果是并发消费，则不会解锁
         */
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                //尝试获取处理队列的消费锁，最多等待1s
                //这是一个本地互斥锁，保证在获取到锁以及发起解锁的过程中，没有线程能消费该队列的消息
                //因为MessageListenerOrderly在消费消息时也需要获取该锁。
                if (pq.getConsumeLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        /*
                         * 延迟的向Broker发送单向请求，Code为UNLOCK_BATCH_MQ，表示请求Broker释放当前消息队列的分布式锁
                         */
                        return this.unlockDelay(mq, pq);
                    } finally {
                        //本地解锁
                        pq.getConsumeLock().unlock();
                    }
                } else {
                    //加锁失败，表示当前消息队列正在消息，不能解锁
                    //那么本次就放弃解锁了，移除消息队列失败，等待下次重新分配消费队列时，再进行移除。
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                            mq,
                            pq.getTryUnlockTimes());
                    //尝试解锁次数+1
                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    /**
     * RebalancePushImpl的方法
     * <p>
     * 延迟的向Broker发送单向请求，Code为UNLOCK_BATCH_MQ，表示请求Broker释放当前消息队列的分布式锁
     *
     * @param mq 要解锁的消息队列
     * @param pq 要解锁的消息队列的处理队列
     * @return 解锁是否成功
     */
    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {
        //如果消费队列中还有剩余消息，则延迟20s解锁
        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            //延迟20s发送解锁请求
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            //立即发送解锁请求
            this.unlock(mq, true);
        }
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Deprecated
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1L;
        try {
            result = computePullFromWhereWithException(mq);
        } catch (MQClientException e) {
            log.warn("Compute consume offset exception, mq={}", mq);
        }
        return result;
    }

    /**
     * RebalancePushImpl的方法
     * <p>
     * 计算该MessageQueue的下一个消息的消费偏移量offset
     * pull模式返回0，push模式则根据consumeFromWhere计算得到
     *
     * @param mq 需要获取offset的消息队列
     * @return offset
     */
    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        long result = -1;
        //获取消费者的ConsumeFromWhere配置，可以通过调用DefaultMQPushConsumer#setConsumeFromWhere方法设置
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        //获取offset管理服务
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            //废弃的配置，默认使用CONSUME_FROM_LAST_OFFSET的逻辑
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
                /*
                 * 消费者组第一次启动时从最后的位置消费，后续再启动接着上次消费的进度开始消费
                 */
            case CONSUME_FROM_LAST_OFFSET: {
                /*
                 * 首先读取上次消费进度，pull模式从本地文件读取，push模式从broker读取
                 */
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                //看作是第一次启动，从最后的位置开始消费
                else if (-1 == lastOffset) {
                    //如果是重试topic，则返回0
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            //请求broker，获取mq对应ConsumeQueue的最大偏移量，即最新消息索引点位
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
            /*
             * 消费者组第一次启动时从最开始的位置消费，后续再启动接着上次消费的进度开始消费
             */
            case CONSUME_FROM_FIRST_OFFSET: {
                /*
                 * 首先读取上次消费进度，pull模式从本地文件读取，push模式从broker读取
                 */
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                //看作是第一次启动，从最开始的位置开始消费
                else if (-1 == lastOffset) {
                    result = 0L;
                } else {
                    result = -1;
                }
                break;
            }
            /*
             * 消费者组第一次启动时消费在指定时间戳后产生的消息，后续再启动接着上次消费的进度开始消费
             */
            case CONSUME_FROM_TIMESTAMP: {
                /*
                 * 首先读取上次消费进度，pull模式从本地文件读取，push模式从broker读取
                 */
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                //看作是第一次启动，从指定时间戳后产生的消息的位置开始消费
                else if (-1 == lastOffset) {
                    //对于重试消息，那么获取mq对应ConsumeQueue的最大偏移量，即最新消息索引点位
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    } else {
                        try {
                            //解析时间
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                    UtilAll.YYYYMMDDHHMMSS).getTime();
                            //查询指定时间戳之后的消息点位
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }

            default:
                break;
        }

        return result;
    }

    /**
     * 分发处理消息
     *
     * @param pullRequestList
     */
    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        //遍历拉去请求
        for (PullRequest pullRequest : pullRequestList) {
            //将请求存入PullMessageService服务的pullRequestQueue集合中，后续异步的消费，执行拉取消息的请求
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }
}
