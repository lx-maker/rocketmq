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
package org.apache.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

public class DefaultMQProducerImpl implements MQProducerInner {
    private final InternalLogger log = ClientLogger.getLog();
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final ArrayList<EndTransactionHook> endTransactionHookList = new ArrayList<EndTransactionHook>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<CheckForbiddenHook>();
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private ExecutorService asyncSenderExecutor;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    /**
     * @param defaultMQProducer defaultMQProducer
     * @param rpcHook           rpc钩子
     */
    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        //保存defaultMQProducer和rpcHook
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;
        /*
         * 异步发送消息的线程池队列
         */
        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
        /*
         * 默认的异步发送消息的线程池
         * 核心线程和最大线程数量都是当前服务器的可用线程数
         */
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), 1000 * 60, TimeUnit.MILLISECONDS, this.asyncSenderThreadPoolQueue, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(), checkForbiddenHookList.size());
    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 初始化事务环境
     */
    public void initTransactionEnv() {
        //获取内部的TransactionMQProducer
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        //如果有自定义的事务线程池，那么同时使用该线程池作为事务回查线程池
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        }
        //如果没有自定义的事务线程池，那么创建一个单线程的线程池作为事务回查线程池
        else {
            //事务回查消息的阻塞队列，最大长度2000
            this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
            //默认事务回查线程池
            this.checkExecutor = new ThreadPoolExecutor(
                    //核心线程数1
                    producer.getCheckThreadPoolMinSize(),
                    //最大线程数1
                    producer.getCheckThreadPoolMaxSize(), 1000 * 60, TimeUnit.MILLISECONDS, this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void registerEndTransactionHook(final EndTransactionHook hook) {
        this.endTransactionHookList.add(hook);
        log.info("register endTransaction Hook, {}", hook.hookName());
    }

    /**
     * DefaultMQProducerImpl的启动方法
     */
    public void start() throws MQClientException {
        this.start(true);
    }

    /**
     * DefaultMQProducerImpl的启动方法
     *
     * @param startFactory
     * @throws MQClientException
     */
    public void start(final boolean startFactory) throws MQClientException {
        //根据服务状态选择走不同的代码分支
        switch (this.serviceState) {
            /**
             * 服务仅仅创建，而不是启动状态，那么启动服务
             */
            case CREATE_JUST:
                //首先修改服务状态为服务启动失败，如果最终启动成功则再修改为RUNNING
                this.serviceState = ServiceState.START_FAILED;
                /*
                 * 1 检查生产者的配置信息
                 * 主要是检查ProducerGroup是否符合规范，
                 * 如果ProducerGroup为空，或者长度大于255个字符，或者包含非法字符（正常的匹配模式为 ^[%|a-zA-Z0-9_-]+$），或者生产者组名为默认组名DEFAULT_PRODUCER
                 * 满足以上任意条件都校验不通过抛出异常。
                 */
                this.checkConfig();
                //如果ProducerGroup不是CLIENT_INNER_PRODUCER，那么将修改当前的instanceName为当前进程pid，PID就是服务的进程号。
                //CLIENT_INNER_PRODUCER是客户端内部的生产者组名，该生产者用于发送消息回退请求
                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }
                /*
                 * 2 获取MQClientManager实例，然后根据clientId获取或者创建CreateMQClientInstance实例，并赋给mQClientFactory变量
                 */
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);
                /*
                 * 3 将当前生产者注册到MQClientInstance实例的producerTable属性中
                 */
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                //如果注册失败，那么设置服务属性为CREATE_JUST，并抛出异常
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup() + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
                }
                //添加一个默认topic “TBW102”，将会在isAutoCreateTopicEnable属性开启时在broker上自动创建，RocketMQ会基于该Topic的配置创建新的Topic
                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());
                /*
                 * 4 启动CreateMQClientInstance客户端通信实例
                 * netty服务、各种定时任务、拉取消息服务、rebalanceService服务
                 */
                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(), this.defaultMQProducer.isSendMessageWithVIPChannel());
                //服务状态改为RUNNING
                this.serviceState = ServiceState.RUNNING;
                break;
            /**
             * 服务状态是其他的，那么抛出异常，即start方法仅能调用一次
             */
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
            default:
                break;
        }
        /*
         * 5 发送心跳信息给所有broker
         */
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        /*
         * 6 启动一个定时任务，移除超时的请求，并执行异常回调，任务间隔1s
         */
        RequestFutureHolder.getInstance().startScheduledTask(this);

    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.", null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }
                RequestFutureHolder.getInstance().shutdown(this);
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * @deprecated This method will be removed in the version 5.0.0 and {@link DefaultMQProducerImpl#getCheckListener} is recommended.
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 检查事务状态
     *
     * @param addr   broker地址
     * @param msg    half消息
     * @param header 请求头
     */
    @Override
    public void checkTransactionState(final String addr, final MessageExt msg, final CheckTransactionStateRequestHeader header) {
        /*
         * 创建了一个线程任务
         */
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                //获取检查监听器，目前这个监听器已不推荐使用
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                //获取事务监听器，推荐使用该监听器
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    //检查状态
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        //如果存在事务检查监听器，现在一般都没有使用这个组件
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        }
                        //如果存在事务检查监听器，现在一般都使用这个组件
                        else if (transactionListener != null) {
                            log.debug("Used new check API in transaction message");
                            //执行事务监听器的checkLocalTransaction方法，用于检查本地事务，返回事务状态
                            //可以从参数message中获取事务id，进而进行一系列操作
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        } else {
                            log.warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }
                    /*
                     * 处理事务状态
                     */
                    this.processTransactionState(localTransactionState, group, exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            /**
             * 处理事务状态
             *
             * @param localTransactionState 本地事务状态
             * @param producerGroup 生产者组
             * @param exception 抛出的异常
             */
            private void processTransactionState(final LocalTransactionState localTransactionState, final String producerGroup, final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                //half消息的commitLogOffset
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                //half消息的consumeQueueOffset
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                //设置msgId和transactionId，一般他们都是uniqueKey
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                //根据返回的本地事务状态，设置commitOrRollback属性
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }
                //执行钩子函数，一般没有钩子
                doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

                try {
                    //发送结束事务单向请求
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark, 3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };
        /*
         * 通过事务检查线程池执行事务回查的线程任务
         */
        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);
        Validators.isSystemTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    /**
     * DefaultMQProducerImpl的方法
     */
    private void makeSureStateOK() throws MQClientException {
        //服务状态不是RUNNING，那么抛出MQClientException异常。
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        //该方法内部又调用另一个send方法，设置超时时间参数，默认3000ms。
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @param msg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException {
        //调用起始时间
        final long beginStartTime = System.currentTimeMillis();
        //获取异步发送执行器线程池
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            /*
             * 使用线程池异步的执行sendDefaultImpl方法，即异步发送消息
             */
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    /*
                     * 发送之前计算超时时间，如果超时则不发送，直接执行回调函数onException方法
                     */
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            //调用sendDefaultImpl方法执行发送操作
                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                        } catch (Exception e) {
                            //抛出异常，执行回调函数onException方法
                            sendCallback.onException(e);
                        }
                    } else {
                        //超时，执行回调函数onException方法
                        sendCallback.onException(new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 选择一个消息队列
     *
     * @param tpInfo         topic信息
     * @param lastBrokerName 上次使用过的broker
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //调用mqFaultStrategy#selectOneMessageQueue方法
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    /**
     * DefaultMQProducerImpl的方法
     *
     * @param brokerName     brokerName
     * @param currentLatency 当前延迟
     * @param isolation      是否使用默认隔离
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //调用MQFaultStrategy#updateFaultItem方法
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException("No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    /**
     * DefaultMQProducerImpl的方法
     *
     * @param msg               方法
     * @param communicationMode 通信模式
     * @param sendCallback      回调方法
     * @param timeout           超时时间
     */
    private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        /*
         * 1 确定此producer的服务状态正常，如果服务状态不是RUNNING，那么抛出异常
         */
        this.makeSureStateOK();
        /*
         * 2 校验消息的合法性
         */
        Validators.checkMessage(msg, this.defaultMQProducer);
        //生成本次调用id
        final long invokeID = random.nextLong();
        //开始时间戳
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        //结束时间戳
        long endTimestamp = beginTimestampFirst;
        /*
         * 3 尝试查找消息的一个topic路由，用以发送消息
         */
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        //找到有效的topic信息
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            /*
             * 4 计算发送消息的总次数
             * 同步模式为3，即默认允许重试2次，可更改重试次数；其他模式为1，即不允许重试，不可更改
             */
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;
            //记录每一次重试时候发送消息目标Broker名字的数组
            String[] brokersSent = new String[timesTotal];
            /*
             * 在循环中，发送消息，包含消息重试的逻辑，总次数默认不超过3
             */
            for (; times < timesTotal; times++) {
                //上次使用过的broker，可以为空，表示第一次选择
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                /*
                 * 5 选择一个消息队列MessageQueue
                 */
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    mq = mqSelected;
                    //设置brokerName
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        //调用的开始时间
                        beginTimestampPrev = System.currentTimeMillis();
                        //如果还有可调用次数，那么
                        if (times > 0) {
                            //在重新发送期间用名称空间重置topic
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }
                        //现在调用的开始时间 减去 开始时间，判断时候在调用发起之前就超时了
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        //如果已经超时了，那么直接结束循环，不再发送
                        //即超时的时候，即使还剩下重试次数，也不会再继续重试
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }
                        /*
                         * 6 异步、同步、单向发送消息
                         */
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        //方法调用结束时间戳
                        endTimestamp = System.currentTimeMillis();
                        /*
                         * 7 更新本地错误表缓存数据，用于延迟时间的故障转移的功能
                         */
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        /*
                         * 8 根据发送模式执行不同的处理
                         */
                        switch (communicationMode) {
                            //异步和单向模式直接返回null
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                //同步模式，如果开启了retryAnotherBrokerWhenNotStoreOK开关，那么如果不是返回SEND_OK状态，则仍然会执行重试发送
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }
                                //如果发送成功，则返回
                                return sendResult;
                            default:
                                break;
                        }
                    } catch (RemotingException e) {
                        //RemotingException异常，会执行重试
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQClientException e) {
                        //MQClientException异常，会执行重试
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQBrokerException e) {
                        //MQBrokerException异常
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        //如果返回的状态码属于一下几种，则支持重试：
                        //ResponseCode.TOPIC_NOT_EXIST,
                        //ResponseCode.SERVICE_NOT_AVAILABLE,
                        //ResponseCode.SYSTEM_ERROR,
                        //ResponseCode.NO_PERMISSION,
                        //ResponseCode.NO_BUYER_ID,
                        //ResponseCode.NOT_IN_CURRENT_UNIT

                        if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
                            continue;
                        } else {
                            //其他状态码不支持重试，如果有结果则返回，否则直接抛出异常
                            if (sendResult != null) {
                                return sendResult;
                            }

                            throw e;
                        }
                    } catch (InterruptedException e) {
                        //InterruptedException异常，不会执行重试
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());

                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }
            /*
             * 抛出异常的操作
             */
            if (sendResult != null) {
                return sendResult;
            }

            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s", times, System.currentTimeMillis() - beginTimestampFirst, msg.getTopic(), Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        validateNameServerSetting();

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO), null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 查找指定topic的推送信息
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        //尝试直接从producer的topicPublishInfoTable中获取topic信息
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        //如果没有获取到有效信息，
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            //那么立即创建一个TopicPublishInfo
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            //立即从nameServer同步此topic的路由配置信息，并且更新本地缓存
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            //再次获取topicPublishInfo
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        //如果找到的路由信息是可用的，直接返回
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            //再次从nameServer同步topic的数据，不过这次使用默认的topic “TBW102”去找路由配置信息作为本topic参数信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     * DefaultMQProducerImpl的方法
     * 发送消息
     *
     * @param msg               消息
     * @param mq                mq
     * @param communicationMode 发送模式
     * @param sendCallback      发送回调
     * @param topicPublishInfo  topic信息
     * @param timeout           超时时间
     * @return 发送结果
     */
    private SendResult sendKernelImpl(final Message msg, final MessageQueue mq, final CommunicationMode communicationMode, final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //开始时间
        long beginStartTime = System.currentTimeMillis();
        /*
         * 1 根据brokerName从brokerAddrTable中查找broker地址
         */
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        //如果本地找不到 broker 的地址
        if (null == brokerAddr) {
            /*
             * 2 从nameServer远程拉取配置，并更新本地缓存
             * 该方法此前就学习过了
             */
            tryToFindTopicPublishInfo(mq.getTopic());
            //再次获取地址
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        SendMessageContext context = null;
        if (brokerAddr != null) {
            /*
             * 3 vip通道判断
             */
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            byte[] prevBody = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                /*
                 * 4 如果不是批量消息，那么尝试生成唯一uniqId，即UNIQ_KEY属性。MessageBatch批量消息在生成时就已经设置uniqId
                 * uniqId也被称为客户端生成的msgId，从逻辑上代表唯一一条消息
                 */
                if (!(msg instanceof MessageBatch)) {
                    MessageClientIDSetter.setUniqID(msg);
                }
                /*
                 * 设置nameSpace为实例Id
                 */
                boolean topicWithNamespace = false;
                if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                    topicWithNamespace = true;
                }
                //消息标识符
                int sysFlag = 0;
                //消息压缩标识
                boolean msgBodyCompressed = false;
                /*
                 * 5 尝试压缩消息
                 */
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    msgBodyCompressed = true;
                }
                //事务消息标志，prepare消息
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }
                /*
                 * 6 如果存在CheckForbiddenHook，则执行checkForbidden方法
                 * 为什么叫禁止钩子呢，可能是想要使用者将不可发送消息的检查放在这个钩子函数里面吧（猜测）
                 */
                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }
                /*
                 * 7 如果存在SendMessageHook，则执行sendMessageBefore方法
                 */
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }
                /*
                 * 8 设置请求头信息
                 */
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());
                requestHeader.setBatch(msg instanceof MessageBatch);
                //针对重试消息的处理
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    //获取消息重新消费次数属性值
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        //将重新消费次数设置到请求头中，并且清除该属性
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }
                    //获取消息的最大重试次数属性值
                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        //将最大重新消费次数设置到请求头中，并且清除该属性
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }
                /*
                 * 9 根据不同的发送模式，发送消息
                 */
                SendResult sendResult = null;
                switch (communicationMode) {
                    /*
                     * 异步发送模式
                     */
                    case ASYNC:
                        /*
                         * 首先克隆并还原消息
                         *
                         * 该方法的finally中已经有还原消息的代码了，为什么在异步发送消息之前，还要先还原消息呢？
                         *
                         * 因为异步发送时 finally 重新赋值的时机并不确定，有很大概率是在第一次发送结束前就完成了 finally 中的赋值，
                         * 因此在内部重试前 msg.body 大概率已经被重新赋值过，而 onExceptionImpl 中的重试逻辑 MQClientAPIImpl.sendMessageAsync 不会再对数据进行压缩，
                         * 简言之，在异步发送的情况下，如果调用 onExceptionImpl 内部的重试，有很大概率发送的是无压缩的数据
                         */
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        //如果开启了消息压缩
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            //克隆一个message
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            //恢复原来的消息体
                            msg.setBody(prevBody);
                        }
                        //如果topic整合了namespace
                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            //还原topic
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }
                        /*
                         * 发送消息之前，进行超时检查，如果已经超时了那么取消本次发送操作，抛出异常
                         */
                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        /*
                         * 10 发送异步消息
                         */
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(brokerAddr, mq.getBrokerName(), tmpMessage, requestHeader, timeout - costTimeAsync, communicationMode, sendCallback, topicPublishInfo, this.mQClientFactory, this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(), context, this);
                        break;
                    /*
                     * 单向、同步发送模式
                     */
                    case ONEWAY:
                    case SYNC:
                        /*
                         * 发送消息之前，进行超时检查，如果已经超时了那么取消本次发送操作，抛出异常
                         */
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        /*
                         * 10 发送单向、同步消息
                         */
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(brokerAddr, mq.getBrokerName(), msg, requestHeader, timeout - costTimeSync, communicationMode, context, this);
                        break;
                    default:
                        assert false;
                        break;
                }
                /*
                 * 9 如果存在SendMessageHook，则执行sendMessageAfter方法
                 */
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }
                //返回执行结果
                return sendResult;

                //如果抛出了异常，如果存在SendMessageHook，则执行sendMessageAfter方法
            } catch (RemotingException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (InterruptedException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                /*
                 * 对消息进行恢复
                 * 1、因为客户端可能还需要查看原始的消息内容，如果是压缩消息，则无法查看
                 * 2、另外如果第一次压缩后消息还是大于4k，如果不恢复消息，那么客户端使用该message重新发送的时候，还会进行一次消息压缩
                 */
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    /**
     * DefaultMQProducerImpl的方法
     */
    private boolean tryToCompressMessage(final Message msg) {
        //如果是批量消息，那么不进行压缩
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            //如果消息长度大于4K
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    //进行压缩，使用的JDK自带的压缩类
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        //重新设置到body中
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    public boolean hasEndTransactionHook() {
        return !this.endTransactionHookList.isEmpty();
    }

    public void executeEndTransactionHook(final EndTransactionContext context) {
        if (!this.endTransactionHookList.isEmpty()) {
            for (EndTransactionHook hook : this.endTransactionHookList) {
                try {
                    hook.endTransaction(context);
                } catch (Throwable e) {
                    log.warn("failed to executeEndTransactionHook", e);
                }
            }
        }
    }

    public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state, boolean fromTransactionCheck) {
        if (hasEndTransactionHook()) {
            EndTransactionContext context = new EndTransactionContext();
            context.setProducerGroup(defaultMQProducer.getProducerGroup());
            context.setBrokerAddr(brokerAddr);
            context.setMessage(msg);
            context.setMsgId(msgId);
            context.setTransactionId(msg.getTransactionId());
            context.setTransactionState(state);
            context.setFromTransactionCheck(fromTransactionCheck);
            executeEndTransactionHook(context);
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            //调用另一个sendDefaultImpl方法，设置消息发送模式为ONEWAY，即单向；设置回调函数为null；设置超时时间参数，默认3000ms
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        makeSureStateOK();
                        Validators.checkMessage(msg, defaultMQProducer);

                        if (!msg.getTopic().equals(mq.getTopic())) {
                            throw new MQClientException("message's topic not equal mq's topic", null);
                        }
                        long costTime = System.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime) {
                            try {
                                sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null, timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } else {
                            sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }

                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(Message msg, MessageQueueSelector selector, Object arg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                List<MessageQueue> messageQueueList = mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue threw exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            try {
                                sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 发送事务消息
     *
     * @param msg                      要发送的事务胸袭
     * @param localTransactionExecuter 本地事务执行器，一般都是null
     * @param arg                      本地事务执行参数
     * @return
     * @throws MQClientException
     */
    public TransactionSendResult sendMessageInTransaction(final Message msg, final LocalTransactionExecuter localTransactionExecuter, final Object arg) throws MQClientException {
        //获取设置的transactionListener，不可为null
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        //忽略DelayTimeLevel参数，事务消息不支持延迟消息，将PROPERTY_DELAY_TIME_LEVEL（DELAY）属性清除
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }
        //校验消息的合法性
        Validators.checkMessage(msg, this.defaultMQProducer);


        SendResult sendResult = null;
        //设置事务half半消息标志，设置PROPERTY_TRANSACTION_PREPARED属性为true
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        //设置PROPERTY_PRODUCER_GROUP属性，为当前生产者所属的生产者组
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        /*
         * 第一阶段 发送half半消息
         */
        try {
            //调用defaultMQProducerImpl#send同步发送half半消息
            sendResult = this.send(msg);
        } catch (Exception e) {
            //如果出现异常，那么直接抛出
            throw new MQClientException("send message Exception", e);
        }
        /*
         * 处理发送half半消息的结果，执行本地事务
         */
        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            //如果发送成功
            case SEND_OK: {
                try {
                    //获取事务id
                    if (sendResult.getTransactionId() != null) {
                        //设置__transactionId__属性为事务id，这个属性目前没用到
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    //获取生产者客户端生成的uniqId。uniqId也被称为msgId，从逻辑上代表客户端生成的唯一一条消息
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        //设置msg的事务id为uniqId
                        msg.setTransactionId(transactionId);
                    }
                    //如果存在本地事务执行器，现在一般都没有使用这个组件
                    if (null != localTransactionExecuter) {
                        //那么通过本地事务执行器执行本地事务
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    }
                    //否则，如果存在事务监听器，现在一般都使用事务监听器
                    else if (transactionListener != null) {
                        log.debug("Used new transaction API");
                        //通过transactionListener#executeLocalTransaction方法执行本地事务，获取本地事务状态
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    //如果返回null，那么算作UNKNOW状态
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }
                    //如果事务状态不是COMMIT_MESSAGE，那么输出日志
                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    //记录异常
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;

            //消息发送成功但是服务器刷盘超时。
            case FLUSH_DISK_TIMEOUT:
                //消息发送成功，但是服务器同步到Slave时超时。
            case FLUSH_SLAVE_TIMEOUT:
                //消息发送成功，但是此时Slave不可用。
            case SLAVE_NOT_AVAILABLE:
                //如果是以上状态，设置本地事务状态为ROLLBACK_MESSAGE，即回滚
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }
        /*
         * 第二阶段 事务的commit或者rollback
         */
        try {
            this.endTransaction(msg, sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        //返回事务消息发送结果
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //调用另一个send方法，设置超时时间参数，默认3000ms
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * DefaultMQProducerImpl的方法
     * <p>
     * 结束事务，事务的commit或者rollback
     *
     * @param msg                   事务消息
     * @param sendResult            发送结果
     * @param localTransactionState 本地事务状态
     * @param localException        本地事务执行抛出的异常
     */
    public void endTransaction(final Message msg, final SendResult sendResult, final LocalTransactionState localTransactionState, final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        final MessageId id;
        //解码消息id，包含broker地址和offset
        //首先获取offsetMsgId，如果存在则设置为消息id，这是真正的Message Id，是broker生成的唯一id
        //如果没有offsetMsgId，那么设置msgId为消息id，这是客户端生成的唯一id，即uniqId
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        //从sendResult获取事务id，一般都是null
        String transactionId = sendResult.getTransactionId();
        //获取broker地址
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
        //创建结束事务请求头
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        //设置事务id
        requestHeader.setTransactionId(transactionId);
        //设置消息的commitLog偏移量
        requestHeader.setCommitLogOffset(id.getOffset());
        /*
         * 根据本地事务状态，设置broker事务消息提交或者回滚
         */
        switch (localTransactionState) {
            //本地事务成功
            case COMMIT_MESSAGE:
                //提交
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            //本地事务回滚
            case ROLLBACK_MESSAGE:
                //回滚
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            //未知状态
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }
        //执行钩子函数，一般没有钩子
        doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
        //设置生产者组
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        //事务消息在queue中的偏移量
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        //设置msgId，即uniqId
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        //发送结束事务单向请求
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    /**
     * DefaultMQProducerImpl的方法
     */
    public ExecutorService getAsyncSenderExecutor() {
        //如果asyncSenderExecutor不为null，即如果手动设置了异步发送线程池，那么返回手动设置的线程池
        //否则，返回默认的异步发送线程池defaultAsyncSenderExecutor，核心线程和最大线程数量都是当前服务器的可用线程数
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    /**
     * DefaultMQProducerImpl的方法
     *
     * @param msg     消息
     * @param timeout 超时时间，毫秒值
     */
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //调用另一个sendDefaultImpl方法，设置消息发送模式为SYNC，即同步；设置回调函数为null
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(Message msg, long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg, final RequestCallback requestCallback, final long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture, long cost) throws InterruptedException, RequestTimeoutException, MQClientException {
        Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
        if (responseMessage == null) {
            if (requestResponseFuture.isSendRequestOk()) {
                throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
            } else {
                throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
            }
        }
        return responseMessage;
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getmQClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getmQClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}
