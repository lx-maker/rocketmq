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
package org.apache.rocketmq.broker.transaction;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTransactionalMessageCheckListener {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    //queue nums of topic TRANS_CHECK_MAX_TIME_TOPIC
    protected final static int TCMT_QUEUE_NUMS = 1;

    /**
     * 异步回查线程池
     */
    private static ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Transaction-msg-check-thread");
            return thread;
        }
    }, new CallerRunsPolicy());

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * AbstractTransactionalMessageCheckListener的方法
     * <p>
     * 发送回查消息
     *
     * @param msgExt half消息
     */
    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        //构建回查消息请求头
        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
        checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
        //migId和transactionId是同一个
        checkTransactionStateRequestHeader.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
        //消息队列偏移量
        checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
        //设置真实topic和queueId
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);
        //获取生产者组
        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        //选择该组里面的一个活跃的生产者，轮询策略
        Channel channel = brokerController.getProducerManager().getAvailableChannel(groupId);
        if (channel != null) {
            //向producer发起事务状态回查请求
            brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
        } else {
            LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    /**
     * AbstractTransactionalMessageCheckListener的方法
     * <p>
     * 发起回查
     *
     * @param msgExt half消息
     */
    public void resolveHalfMsg(final MessageExt msgExt) {
        //通过内部的executorService线程池发起一个异步的回查
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    //异步的发送回查消息
                    sendCheckMessage(msgExt);
                } catch (Exception e) {
                    LOGGER.error("Send check message error!", e);
                }
            }
        });
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void shutDown() {
        executorService.shutdown();
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
