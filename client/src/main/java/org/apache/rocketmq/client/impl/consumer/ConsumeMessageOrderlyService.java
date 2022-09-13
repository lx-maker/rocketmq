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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
            Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        String consumeThreadPrefix = null;
        if (consumerGroup.length() > 100) {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup.substring(0, 100)).append("_").toString();
        } else {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
        }
        /*
         * 并发消费线程池
         * 最小、最大线程数默认20，阻塞队列为无界阻塞队列LinkedBlockingQueue
         */
        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl(consumeThreadPrefix));
        //单线程的延迟任务线程池，用于定时执行锁定请求以及延迟提交新的消费请求
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * 启动服务
     */
    public void start() {
        //如果是集群模式
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            //启动一个定时任务，启动后1s执行，后续每20s执行一次
            //尝试对所有分配给当前consumer的队列请求broker端的消息队列锁，保证同时只有一个消费端可以消费。
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        //定期锁定所有消息队列
                        ConsumeMessageOrderlyService.this.lockMQPeriodically();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
                    }
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageOrderlyService.this.consumerGroup,
                    msgs,
                    mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * 提交顺序消费请求
     *
     * @param msgs             拉取到的消息
     * @param processQueue     处理队列
     * @param messageQueue     消息队列
     * @param dispathToConsume 是否分发消费
     */
    @Override
    public void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume) {
        //如果允许分发消费
        if (dispathToConsume) {
            //构建消费请求，没有将消息放进去，消费消费会自动拉取treemap中的消息
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            //将请求提交到consumeExecutor线程池中进行消费
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * <p>
     * 锁定所有消息队列
     */
    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            //锁定所有消息队列
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * <p>
     * 集群模式下，尝试延迟加锁并重新消费
     *
     * @param mq           消息队列
     * @param processQueue 处理队列
     * @param delayMills   延迟时间，如果在循环中，发现没有锁定或者锁过期，那么延迟10ms；如果在最开始判断的时候，就发现处理队列没有被丢弃，但是也没有锁定或者锁过期，那么延迟100ms
     */
    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
                                         final long delayMills) {
        //构建一个延迟线程任务，通过延迟线程池服务在给定的延迟时间之后执行
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                //尝试请求broker锁定该mq
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    //如果锁定成功，那么调用submitConsumeRequestLater方法延迟提交消费请求，延迟10ms。
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    //如果锁定失败，那么同样调用submitConsumeRequestLater方法延迟提交消费请求，但是延迟3000ms，即3s。
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * <p>
     * 延迟提交消费请求
     *
     * @param processQueue      处理队列
     * @param messageQueue      消息队列
     * @param suspendTimeMillis 延迟时间
     */
    private void submitConsumeRequestLater(
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final long suspendTimeMillis
    ) {
        //如果延迟时间为-1，则将DefaultMQPushConsumer.suspendCurrentQueueTimeMillis属性作为延迟时间，默认1s
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }
        //最少延迟10ms，最多延迟30000ms
        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }
        //构建一个延迟线程任务，通过延迟线程池服务在给定的延迟时间之后执行submitConsumeRequest方法
        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                //提交的消息为null，dispathToConsume为true，也就是说一定会构建一个新的ConsumeRequest并且将请求提交到consumeExecutor线程池中进行消费
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * <p>
     * 处理消费结果
     *
     * @param msgs           消息
     * @param status         消费状态
     * @param context        上下文
     * @param consumeRequest 消费请求
     * @return 消费结果，是否继续消费
     */
    public boolean processConsumeResult(
            final List<MessageExt> msgs,
            final ConsumeOrderlyStatus status,
            final ConsumeOrderlyContext context,
            final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        //如果context设置为自动提交，context默认都是true，除非在业务中手动改为false
        if (context.isAutoCommit()) {
            switch (status) {
                //使用废弃的状态，默认算作SUCCESS
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                            consumeRequest.getMessageQueue());
                    //消费成功
                case SUCCESS:
                    /*
                     * 通过处理队列提交offset，这里仅仅是更新本地内存的消息缓存信息
                     */
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    //统计
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                //消费失败
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    //统计
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    /*
                     * 校验是否达到最大重试次数，可以通过DefaultMQPushConsumer#maxReconsumeTimes属性配置，默认无上限，即Integer.MAX_VALUE
                     */
                    if (checkReconsumeTimes(msgs)) {
                        //没有到达最大重试次数
                        //标记消息等待再次消费
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        //延迟提交新的消费请求，默认suspendTimeMillis为-1，即延迟1s后重新消费
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        //本消费请求消费结束不会继续消费
                        continueConsume = false;
                    } else {
                        //达到了最大重试次数，那么提交消息，算作成功
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
        }
        //如果context设置为手动提交，一般都不会走这个逻辑
        else {
            switch (status) {
                //消费成功
                case SUCCESS:
                    //仅仅是统计数据
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                //只有返回COMMIT，那么才会提交消息，这里仅仅是更新本地内存的消息缓存信息
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                //ROLLBACK回滚
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                //消费失败稍后再试
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    //统计
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    /*
                     * 校验是否达到最大重试次数，可以通过DefaultMQPushConsumer#maxReconsumeTimes属性配置，默认无上限，即Integer.MAX_VALUE
                     */
                    if (checkReconsumeTimes(msgs)) {
                        //没有到达最大重试次数
                        //标记消息等待再次消费
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        //延迟提交新的消费请求，默认suspendTimeMillis为-1，即延迟1s后重新消费
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        //本消费请求消费结束不会继续消费
                        continueConsume = false;
                    }
                    //达到了最大重试次数，也不会提交消息
                    break;
                default:
                    break;
            }
        }
        /*
         * 如果偏移量大于等于0并且处理队列没有被丢弃，调用OffsetStore# updateOffset方法，尝试更新内存中的offsetTable中的最新偏移量信息
         * 第三个参数是否仅单调增加offset为false，表示可能会将offset更新为较小的值
         * 这里仅仅是更新内存中的数据，而offset除了在拉取消息时上报broker进行持久化之外，还会定时每5s调用persistAllConsumerOffset定时持久化。
         * 我们在后面Consumer消费进度管理部分会学习相关源码。
         */
        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * 顺序消费调用
     * <p>
     * 校验是否达到最大重试次数，可以通过DefaultMQPushConsumer#maxReconsumeTimes属性配置，默认无上限，即Integer.MAX_VALUE
     *
     * @param msgs
     * @return
     */
    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            //遍历消息
            for (MessageExt msg : msgs) {
                //校验是否达到最大重试次数，可以通过DefaultMQPushConsumer#maxReconsumeTimes属性配置，默认无上限，即Integer.MAX_VALUE
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    //如果达到最大重试次数，设置RECONSUME_TIME属性
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    //通过sendMessageBack发回broker延迟topic
                    if (!sendMessageBack(msg)) {
                        //如果sendMessageBack发送失败
                        //挂起
                        suspend = true;
                        //设置消息的重试次数属性reconsumeTimes+1
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                }
                //如果没有达到最大重试次数
                else {
                    //挂起
                    suspend = true;
                    //设置消息的重试次数属性reconsumeTimes+1
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    /**
     * ConsumeMessageOrderlyService的方法
     * 顺序消费，将重试次数达到最大值的消息发往broker死信队列
     *
     * @param msg 发送的消息
     * @return 是否发送成功
     */
    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            //新构造一个msg
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            //设置重试次数
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            //设置最大重试次数，默认
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            //设置延迟等级PROPERTY_DELAY_TIME_LEVEL属性， 3 + 重试次数
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            //调用DefaultMQProducer#send方法发送消息
            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    class ConsumeRequest implements Runnable {
        //处理队列
        private final ProcessQueue processQueue;
        //消息队列
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        /**
         * ConsumeMessageOrderlyService的内部类ConsumeRequest的方法
         * <p>
         * 执行顺序消费
         */
        @Override
        public void run() {
            //如果处理队列被丢弃，那么直接返回，不再消费，例如负载均衡时该队列被分配给了其他新上线的消费者，尽量避免重复消费
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }
            /*
             * 1 消费消息之前先获取当前messageQueue的本地锁，防止并发
             * 这将导致ConsumeMessageOrderlyService的线程池中的线程将不会同时并发的消费同一个队列
             */
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            /*
             * 阻塞式的获取同步锁，锁对象是一个Object对象，采用原生的synchronized锁定
             */
            synchronized (objLock) {
                /*
                 * 2 如果是广播模式，或者是 （集群模式，并且锁定了processQueue处理队列，并且processQueue处理队列锁没有过期），那么可以消费消息
                 * processQueue处理队列锁定实际上就是在负载均衡的时候向broker申请的消息队列分布式锁，申请成功之后将processQueue.locked属性置为true
                 *
                 * 当前消费者通过RebalanceImpl#rebalanceByTopic分配了新的消息队列之后，对于集群模式的顺序消费会尝试通过RebalanceImpl#lock方法请求broker获取该队列的分布式锁
                 * 同理在ConsumeMessageOrderlyService启动的时候，其对于集群模式则会启动一个定时任务，默认每隔20s调用RebalanceImpl#lockAll方法，请求broker获取所有分配的队列的分布式锁
                 */
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    //消费起始时间
                    final long beginTime = System.currentTimeMillis();
                    /*
                     * 3 循环继续消费，直到超时或者条件不满足退出循环
                     */
                    for (boolean continueConsume = true; continueConsume; ) {
                        //3.1 如果处理队列被丢弃，那么直接返回，不再消费，例如负载均衡时该队列被分配给了其他新上线的消费者，尽量避免重复消费
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            //结束循环，本次消费任务结束
                            break;
                        }
                        //3.2 如果是集群模式，并且没有锁定了processQueue处理队列
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            //对该队列请求broker获取该队列的分布式锁，然后延迟提交消费请求
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            //结束循环，本次消费任务结束
                            break;
                        }
                        //3.3 如果是集群模式，并且processQueue处理队列锁已经过期
                        //客户端对于从broker获取的mq锁，过期时间默认30s，可以通过-Drocketmq.client.rebalance.lockMaxLiveTime参数设置
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            //对该队列请求broker获取该队列的分布式锁，然后延迟提交消费请求
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            //结束循环，本次消费任务结束
                            break;
                        }
                        //计算消费时间
                        long interval = System.currentTimeMillis() - beginTime;
                        //3.4 如果单次消费任务的消费时间大于默认60s，可以通过-Drocketmq.client.maxTimeConsumeContinuously配置启动参数来设置时间
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            //延迟提交新的消费请求
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            //结束循环，本次消费任务结束
                            break;
                        }
                        //获取单次批量消费的数量，默认1，可以通过DefaultMQPushConsumer.consumeMessageBatchMaxSize的属性配置
                        final int consumeBatchSize =
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
                        /*
                         * 3.5 从processQueue内部的msgTreeMap有序map集合中获取offset最小的consumeBatchSize条消息，按顺序从最小的offset返回，保证有序性
                         */
                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                        //重置重试topic，当消息是重试消息的时候，将msg的topic属性从重试topic还原为真实的topic。
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
                        /*
                         * 4 如果拉取到了消息，那么进行消费
                         */
                        if (!msgs.isEmpty()) {
                            //顺序消费上下文
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                            //消费状态
                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            /*
                             * 4.1 如果有钩子，那么执行consumeMessageBefore前置方法
                             */
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                        .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }
                            //起始时间
                            long beginTimestamp = System.currentTimeMillis();
                            //消费返回类型
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                /*
                                 * 4.2 真正消费消息之前再获取processQueue的本地消费锁，保证消息消费时，一个处理队列不会被并发消费
                                 * 从这里可知，顺序消费需要获取三把锁，broker的messageQueue锁，本地的messageQueue锁，本地的processQueue锁
                                 */
                                this.processQueue.getConsumeLock().lock();
                                //如果处理队列被丢弃，那么直接返回，不再消费
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                            this.messageQueue);
                                    //结束循环，本次消费任务结束
                                    break;
                                }
                                /*
                                 * 4.3 调用listener#consumeMessage方法，进行消息消费，调用实际的业务逻辑，返回执行状态结果
                                 * 有四种状态，ConsumeOrderlyStatus.SUCCESS 和 ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT推荐使用
                                 * ConsumeOrderlyStatus.ROLLBACK和ConsumeOrderlyStatus.COMMIT已被废弃
                                 */
                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                                        RemotingHelper.exceptionSimpleDesc(e),
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue), e);
                                //抛出异常之后，设置异常标志位
                                hasException = true;
                            } finally {
                                //解锁
                                this.processQueue.getConsumeLock().unlock();
                            }
                            /*
                             * 4.4 对返回的执行状态结果进行判断处理
                             */
                            //如status为null，或返回了ROLLBACK或者SUSPEND_CURRENT_QUEUE_A_MOMENT状态，那么输出日志
                            if (null == status
                                    || ConsumeOrderlyStatus.ROLLBACK == status
                                    || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                            }
                            //计算消费时间
                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            //如status为null
                            if (null == status) {
                                //如果业务的执行抛出了异常
                                if (hasException) {
                                    //设置returnType为EXCEPTION
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    //设置returnType为RETURNNULL
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            }
                            //如消费时间consumeRT大于等于consumeTimeout，默认15min
                            else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                //设置returnType为TIME_OUT
                                returnType = ConsumeReturnType.TIME_OUT;
                            }
                            //如status为SUSPEND_CURRENT_QUEUE_A_MOMENT，即消费失败
                            else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                //设置returnType为FAILED
                                returnType = ConsumeReturnType.FAILED;
                            }
                            //如status为SUCCESS，即消费成功
                            else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                //设置returnType为SUCCESS，即消费成功
                                returnType = ConsumeReturnType.SUCCESS;
                            }
                            //如果有钩子，则将returnType设置进去
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }
                            //如果status为null
                            if (null == status) {
                                //将status设置为SUSPEND_CURRENT_QUEUE_A_MOMENT，即消费失败
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }
                            /*
                             * 4.5 如果有消费钩子，那么执行钩子函数的后置方法consumeMessageAfter
                             * 我们可以注册钩子ConsumeMessageHook，在消费消息的前后调用
                             */
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                        .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }
                            //增加消费时间
                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                    .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
                            /*
                             * 5 调用ConsumeMessageOrderlyService#processConsumeResult方法处理消费结果，包含重试等逻辑
                             */
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                        } else {
                            //如果没有拉取到消息，那么设置continueConsume为false，将会跳出循环
                            continueConsume = false;
                        }
                    }
                } else {
                    //如果processQueue被丢弃，则直接结束本次消费请求
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }
                    //如果是集群模式，并且没有锁定了processQueue处理队列或者processQueue处理队列锁已经过期
                    //尝试延迟加锁并重新消费
                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
