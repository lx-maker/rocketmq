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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final String TOPIC_QUEUEID_SEPARATOR = "@";
    protected final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    /**
     * 挂起的拉取请求的缓存
     * key为topic@queueId，value挂起的pollRequest
     */
    protected ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * PullRequestHoldService的方法
     * <p>
     * 挂起请求，将请求存入pullRequestTable
     *
     * @param topic       请求的topic
     * @param queueId     请求的队列id
     * @param pullRequest 拉取请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        //构建key： topic@queueId
        String key = this.buildKey(topic, queueId);
        //从缓存里面尝试获取该key的值ManyPullRequest
        //ManyPullRequest是包含多个pullRequest的对象，内部有一个集合
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }
        //存入ManyPullRequest内部的pullRequestList集合中
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder(topic.length() + 5);
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**
     * PullRequestHoldService的方法
     * 处理挂起请求
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        /*
         * 运行时逻辑
         * 如果服务没有停止，则正常执行操作
         */
        while (!this.isStopped()) {
            try {
                /*
                 * 1 阻塞线程
                 * 定时唤醒，或者broker有新消息到达唤醒
                 */
                //如果支持长轮询
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    //那么最长等待5s
                    this.waitForRunning(5 * 1000);
                } else {
                    //否则等待shortPollingTimeMills，默认1s
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }
                //醒了之后继续后面的逻辑
                long beginLockTimestamp = this.systemClock.now();
                /*
                 * 2 检测pullRequestTable中的挂起的请求，如果有新消息到达则执行拉取操作
                 */
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * PullRequestHoldService的方法
     * <p>
     * 检测pullRequestTable中的挂起的请求，如果有新消息到达则执行拉取操作
     */
    protected void checkHoldRequest() {
        //遍历pullRequestTable
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                //获取指定consumeQueue的最大的逻辑偏移量offset
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    //调用notifyMessageArriving方法，尝试通知消息到达
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * PullRequestHoldService的方法
     * 通知消息到达
     *
     * @param topic     请求的topic
     * @param queueId   请求的队列id
     * @param maxOffset consumeQueue的最大的逻辑偏移量offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * PullRequestHoldService的方法
     * 通知消息到达，除了PullRequestHoldService服务定时调用之外，reputMessageService服务发现新消息时可能也会调用该方法
     *
     * @param topic        请求的topic
     * @param queueId      请求的队列id
     * @param maxOffset    consumeQueue的最大的逻辑偏移量offset
     * @param tagsCode     消息的tag的hashCode，注意，如果是定时唤醒，该参数为null
     * @param msgStoreTime 消息存储时间，注意，如果是定时唤醒，该参数为0
     * @param filterBitMap 过滤bitMap，注意，如果是定时唤醒，该参数为null
     * @param properties   参数，注意，如果是定时唤醒，该参数为null
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
                                      long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        //构建key： topic@queueId
        String key = this.buildKey(topic, queueId);
        //从缓存里面尝试获取该key的值ManyPullRequest
        //ManyPullRequest是包含多个pullRequest的对象，内部有一个集合
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        //如果有对应的拉取请求被阻塞，即指定topic以及指定queueId
        if (mpr != null) {
            //获取所有的挂起请求集合
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();
                //遍历挂起的请求
                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    //如果最大偏移量小于等于需要拉取的offset，那么再次获取consumeQueue的最大的逻辑偏移量offset
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }
                    //如果最大偏移量大于需要拉取的offset，那么可以尝试拉取
                    if (newestOffset > request.getPullFromThisOffset()) {
                        /*
                         * 执行消息tagsCode过滤，如果是定时唤醒，由于tagsCode参数为null，那么一定返回true
                         */
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }
                        //如果消息匹配过滤条件
                        if (match) {
                            try {
                                /*
                                 * 通过PullMessageProcessor#executeRequestWhenWakeup重新执行拉取操作
                                 */
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }
                    //如果request等待超时，那么还是会通过PullMessageProcessor#executeRequestWhenWakeup重新执行一次拉取操作
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }
                    /*
                     * 不符合条件并且没有超时的request，重新放回replayList集合中，继续挂起
                     */
                    replayList.add(request);
                }
                //将还需要继续挂起request返回去
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
