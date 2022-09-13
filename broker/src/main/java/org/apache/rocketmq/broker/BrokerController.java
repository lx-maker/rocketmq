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
package org.apache.rocketmq.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.client.ClientHousekeepingService;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.dledger.DLedgerRoleChangeHandler;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filtersrv.FilterServerManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.broker.longpolling.LmqPullRequestHoldService;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.LmqConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.MessageStoreFactory;
import org.apache.rocketmq.broker.plugin.MessageStorePluginContext;
import org.apache.rocketmq.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.QueryMessageProcessor;
import org.apache.rocketmq.broker.processor.ReplyMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.util.ServiceProvider;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;

public class BrokerController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final InternalLogger LOG_PROTECTION = InternalLoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);
    private static final InternalLogger LOG_WATER_MARK = InternalLoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);
    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final ConsumerOffsetManager consumerOffsetManager;
    private final ConsumerManager consumerManager;
    private final ConsumerFilterManager consumerFilterManager;
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    private final MessageArrivingListener messageArrivingListener;
    private final Broker2Client broker2Client;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));
    private final SlaveSynchronize slaveSynchronize;
    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    private final BlockingQueue<Runnable> putThreadPoolQueue;
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    private final BlockingQueue<Runnable> replyThreadPoolQueue;
    private final BlockingQueue<Runnable> queryThreadPoolQueue;
    private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    private final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    private final FilterServerManager filterServerManager;
    private final BrokerStatsManager brokerStatsManager;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private MessageStore messageStore;
    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;
    private TopicConfigManager topicConfigManager;
    private ExecutorService sendMessageExecutor;
    private ExecutorService putMessageFutureExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService replyMessageExecutor;
    private ExecutorService queryMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService consumerManageExecutor;
    private ExecutorService endTransactionExecutor;
    private boolean updateMasterHAServerAddrPeriodically = false;
    private BrokerStats brokerStats;
    private InetSocketAddress storeHost;
    private BrokerFastFailure brokerFastFailure;
    private Configuration configuration;
    private FileWatchService fileWatchService;
    private TransactionalMessageCheckService transactionalMessageCheckService;
    private TransactionalMessageService transactionalMessageService;
    private AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
    private Future<?> slaveSyncFuture;
    private Map<Class, AccessValidator> accessValidatorMap = new HashMap<Class, AccessValidator>();

    public BrokerController(
            final BrokerConfig brokerConfig,
            final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig,
            final MessageStoreConfig messageStoreConfig
    ) {
        //broker的配置
        this.brokerConfig = brokerConfig;
        //作为netty服务端与客户端交互的配置
        this.nettyServerConfig = nettyServerConfig;
        //作为netty客户端与服务端交互的配置
        this.nettyClientConfig = nettyClientConfig;
        //消息存储的配置
        this.messageStoreConfig = messageStoreConfig;
        //消费者偏移量管理器，维护offset进度信息
        this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(this) : new ConsumerOffsetManager(this);
        //topic配置管理器，管理broker中存储的所有topic的配置
        this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(this) : new TopicConfigManager(this);
        //拉取消息处理器，用于处理拉取消息的请求
        this.pullMessageProcessor = new PullMessageProcessor(this);
        //拉取请求挂起服务，处理无消息时push长轮询消费者的挂起等待机制
        this.pullRequestHoldService = messageStoreConfig.isEnableLmq() ? new LmqPullRequestHoldService(this) : new PullRequestHoldService(this);
        //消息送达的监听器，生产者消息到达时通过该监听器触发pullRequestHoldService通知pullRequestHoldService
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
        //消费者id变化监听器
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        //消费者管理类，维护消费者组的注册实例信息以及topic的订阅信息，并对消费者id变化进行监听
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        //消费者过滤管理器，配置文件为：xx/config/consumerFilter.json
        this.consumerFilterManager = new ConsumerFilterManager(this);
        //生产者管理器，包含生产者的注册信息，通过groupName分组
        this.producerManager = new ProducerManager();
        //客户端连接心跳服务，用于定时扫描生产者和消费者客户端，并将不活跃的客户端通道及相关信息移除
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        //处理某些broker到客户端的请求，例如检查生产者的事务状态，重置offset
        this.broker2Client = new Broker2Client(this);
        //订阅分组关系管理器，维护消费者组的一些附加运维信息
        this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(this) : new SubscriptionGroupManager(this);
        //broker对方访问的API，处理broker对外的发起请求，比如向nameServer注册，向master、slave发起的请求
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        //过滤服务管理器，拉取消息过滤
        this.filterServerManager = new FilterServerManager(this);
        //用于从节点，定时向主节点发起请求同步数据，例如topic配置、消费位移等
        this.slaveSynchronize = new SlaveSynchronize(this);

        /*初始化各种阻塞队列。将会被设置到对应的处理不同客户端请求的线程池执行器中*/
        //处理来自生产者的发送消息的请求的队列
        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        //https://github.com/apache/rocketmq/pull/3631
        this.putThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPutThreadPoolQueueCapacity());
        //处理来自消费者的拉取消息的请求的队列
        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        //处理reply消息的请求的队列，RocketMQ4.7.0版本中增加了request-reply新特性，该特性允许producer在发送消息后同步或者异步等待consumer消费完消息并返回响应消息，类似rpc调用效果。
        //即生产者发送了消息之后，可以同步或者异步的收到消费了这条消息的消费者的响应
        this.replyThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
        //处理查询请求的队列
        this.queryThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
        //客户端管理器的队列
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        //消费者管理器的队列，目前没用到
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        //心跳处理的队列
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        //事务消息相关处理的队列
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getEndTransactionPoolQueueCapacity());
        //broker状态管理器，保存Broker运行时状态
        this.brokerStatsManager = messageStoreConfig.isEnableLmq() ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat()) : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());
        //目前没用到
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));
        //broker快速失败服务
        this.brokerFastFailure = new BrokerFastFailure(this);
        //配置类
        this.configuration = new Configuration(
                log,
                BrokerPathConfigHelper.getBrokerConfigPath(),
                this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
        return queryThreadPoolQueue;
    }

    public boolean initialize() throws CloneNotSupportedException {
        /*
         * 1 加载配置文件
         * 尝试从json配置文件或者bak备份文件中加载json字符串，然后反序列化转换为自身内部的属性
         */
        //topic配置文件加载，路径为  {user.home}/store/config/topics.json
        boolean result = this.topicConfigManager.load();
        //消费者消费偏移量配置文件加载，路径为  {user.home}/store/config/consumerOffset.json
        result = result && this.consumerOffsetManager.load();
        //订阅分组配置文件加载，路径为  {user.home}/store/config/subscriptionGroup.json
        result = result && this.subscriptionGroupManager.load();
        //消费者过滤配置文件加载，路径为  {user.home}/store/config/consumerFilter.json
        result = result && this.consumerFilterManager.load();

        /*
         * 如果上一步加载配置全部成功
         * 2 实例化和初始化消息存储服务相关类DefaultMessageStore
         */
        if (result) {
            try {
                //实例化消息存储类DefaultMessageStore
                this.messageStore =
                        new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                                this.brokerConfig);
                //如果启动了enableDLegerCommitLog，就创建DLeger组件DLedgerRoleChangeHandler。默认为false，如果需要开启则该值需要设置为true。
                //在启用enableDLegerCommitLog情况下，broker通过raft协议选主，可以实现主从角色自动切换，这是4.5版本之后的新功能
                //简单的说，如果启动enableDLegerCommitLog，就表示启用 RocketMQ 的容灾机制——自动主从切换，
                if (messageStoreConfig.isEnableDLegerCommitLog()) {
                    DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(this, (DefaultMessageStore) messageStore);
                    ((DLedgerCommitLog) ((DefaultMessageStore) messageStore).getCommitLog()).getdLedgerServer().getdLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
                }
                //broker的统计服务类，保存了broker的一些统计数据
                //例如msgPutTotalTodayNow --现在存储的消息数量，msgPutTotalTodayMorning --今天存储的消息数量
                this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
                //load plugin
                //加载存在的消息存储插件，不必深究
                MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
                this.messageStore = MessageStoreFactory.build(context, this.messageStore);
                //添加一个针对布隆过滤器的消费过滤类
                this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
            } catch (IOException e) {
                result = false;
                log.error("Failed to initialize", e);
            }
        }
        /*
         * 如果上一步加载配置全部成功
         * 3 通过消息存储服务加载消息存储的相关文件，比如commitLog日志文件、consumequeue消息消费队列文件的加载，indexFile索引文件的构建
         * messageStore还会将这些文件的内容加载到内存中，并且完成RocketMQ的数据恢复
         * 这是核broker启动的心步骤之一
         */
        result = result && this.messageStore.load();

        /*
         * 如果上一步加载配置全部成功
         * 3 开始初始化Broker通信层和各种请求执行器
         */
        if (result) {
            /*
             * 4 创建netty远程服务，remotingServer和fastRemotingServer
             */
            //创建broker的netty远程服务，端口为10911，可以用于处理客户端的所有请求。
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            //创建一个broker的netty快速远程服务的配置
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            //设置快速netty远程服务的配置监听的端口号为普通服务的端口-2，默认10909
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            //创建broker的netty快速服务，这就是所谓的快速通道，对应可以处理客户端除了拉取消息之外的所有请求，所谓的VIP端口。
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
            /*
             * 5 创建各种执行器线程池
             */
            //处理发送消息的请求的线程池
            this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getSendMessageThreadPoolNums(),
                    this.brokerConfig.getSendMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.sendThreadPoolQueue,
                    new ThreadFactoryImpl("SendMessageThread_"));
            ///https://github.com/apache/rocketmq/pull/3631
            this.putMessageFutureExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getPutMessageFutureThreadPoolNums(),
                    this.brokerConfig.getPutMessageFutureThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.putThreadPoolQueue,
                    new ThreadFactoryImpl("PutMessageThread_"));
            //处理拉取消息的请求的线程池
            this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getPullMessageThreadPoolNums(),
                    this.brokerConfig.getPullMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.pullThreadPoolQueue,
                    new ThreadFactoryImpl("PullMessageThread_"));
            //处理reply消息的请求的线程池
            this.replyMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
                    this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.replyThreadPoolQueue,
                    new ThreadFactoryImpl("ProcessReplyMessageThread_"));
            //处理查询请求的线程池
            this.queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getQueryMessageThreadPoolNums(),
                    this.brokerConfig.getQueryMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.queryThreadPoolQueue,
                    new ThreadFactoryImpl("QueryMessageThread_"));
            //broker 管理线程池，作为默认处理器的线程池
            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                            "AdminBrokerThread_"));
            //客户端管理器的线程池
            this.clientManageExecutor = new ThreadPoolExecutor(
                    this.brokerConfig.getClientManageThreadPoolNums(),
                    this.brokerConfig.getClientManageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.clientManagerThreadPoolQueue,
                    new ThreadFactoryImpl("ClientManageThread_"));
            //心跳处理的线程池
            this.heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getHeartbeatThreadPoolNums(),
                    this.brokerConfig.getHeartbeatThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.heartbeatThreadPoolQueue,
                    new ThreadFactoryImpl("HeartbeatThread_", true));
            //事务消息相关处理的线程池
            this.endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
                    this.brokerConfig.getEndTransactionThreadPoolNums(),
                    this.brokerConfig.getEndTransactionThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.endTransactionThreadPoolQueue,
                    new ThreadFactoryImpl("EndTransactionThread_"));
            //消费者管理的线程池
            this.consumerManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getConsumerManageThreadPoolNums(), new ThreadFactoryImpl(
                            "ConsumerManageThread_"));

            /*
             * 6 注册处理器
             */
            this.registerProcessor();
            /*
             * 7 启动一系列定时周期任务
             */
            final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
            //每隔24h打印昨天生产和消费的消息数量
            final long period = 1000 * 60 * 60 * 24;
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    } catch (Throwable e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);
            //每隔5s將消费者offset进行持久化，存入consumerOffset.json文件中
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
            //每隔10s將消费过滤信息进行持久化，存入consumerFilter.json文件中
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerFilterManager.persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumer filter error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
            //每隔3m將检查消费者的消费进度
            //当消费进度落后阈值的时候，并且disableConsumeIfConsumerReadSlowly=true(默认false)，就停止消费者消费，保护broker，避免消费积压
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.protectBroker();
                    } catch (Throwable e) {
                        log.error("protectBroker error.", e);
                    }
                }
            }, 3, 3, TimeUnit.MINUTES);
            //每隔1s將打印发送消息线程池队列、拉取消息线程池队列、查询消息线程池队列、结束事务线程池队列的大小以及队列头部元素存在时间
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.printWaterMark();
                    } catch (Throwable e) {
                        log.error("printWaterMark error.", e);
                    }
                }
            }, 10, 1, TimeUnit.SECONDS);
            //每隔1m將打印已存储在commitlog提交日志中但尚未分派到consume queue消费队列的字节数。
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        log.info("dispatch behind commit log {} bytes", BrokerController.this.getMessageStore().dispatchBehindBytes());
                    } catch (Throwable e) {
                        log.error("schedule dispatchBehindBytes error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

            //如果broker的nameServer地址不为null
            if (this.brokerConfig.getNamesrvAddr() != null) {
                //更新nameServer地址
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
                log.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                //如果没有指定nameServer地址，并且允许从地址服务器获取nameServer地址
                //那么每隔2m从nameServer地址服务器拉取最新的nameServer地址并更新
                //要想动态更新nameServer地址，需要指定一个地址服务器的url，并且fetchNamesrvAddrByAddressServer设置为true
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        } catch (Throwable e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }
            //如果没有开启DLeger服务，DLeger开启后表示支持高可用的主从自动切换
            if (!messageStoreConfig.isEnableDLegerCommitLog()) {
                //如果当前broker是slave从节点
                if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                    //根据是否配置了HA地址，来更新HA地址，并设置updateMasterHAServerAddrPeriodically
                    if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                        this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                        this.updateMasterHAServerAddrPeriodically = false;
                    } else {
                        this.updateMasterHAServerAddrPeriodically = true;
                    }
                } else {
                    //如果是主节点，每隔60s將打印主从节点的差异
                    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                BrokerController.this.printMasterAndSlaveDiff();
                            } catch (Throwable e) {
                                log.error("schedule printMasterAndSlaveDiff error.", e);
                            }
                        }
                    }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
                }
            }
            //Tls传输相关配置，通信安全的文件监听模块，用来观察网络加密配置文件的更改
            //默认是PERMISSIVE，因此会进入代码块
            if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
                // Register a listener to reload SslContext
                try {
                    //实例化文件监听服务,并且初始化事务消息服务。
                    fileWatchService = new FileWatchService(
                            new String[]{
                                    TlsSystemConfig.tlsServerCertPath,
                                    TlsSystemConfig.tlsServerKeyPath,
                                    TlsSystemConfig.tlsServerTrustCertPath
                            },
                            new FileWatchService.Listener() {
                                boolean certChanged, keyChanged = false;

                                @Override
                                public void onChanged(String path) {
                                    if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                        log.info("The trust certificate changed, reload the ssl context");
                                        reloadServerSslContext();
                                    }
                                    if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                        certChanged = true;
                                    }
                                    if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                        keyChanged = true;
                                    }
                                    if (certChanged && keyChanged) {
                                        log.info("The certificate and private key changed, reload the ssl context");
                                        certChanged = keyChanged = false;
                                        reloadServerSslContext();
                                    }
                                }

                                private void reloadServerSslContext() {
                                    ((NettyRemotingServer) remotingServer).loadSslContext();
                                    ((NettyRemotingServer) fastRemotingServer).loadSslContext();
                                }
                            });
                } catch (Exception e) {
                    log.warn("FileWatchService created error, can't load the certificate dynamically");
                }
            }
            /*
             * 8 初始化事务消息相关服务
             */
            initialTransaction();
            /*
             * 9 初始化权限相关服务
             */
            initialAcl();
            /*
             * 10 初始化RPC调用的钩子函数
             */
            initialRpcHooks();
        }
        return result;
    }

    /**
     * BrokerController的方法
     * 初始化事务消息相关服务
     */
    private void initialTransaction() {
        //基于Java的SPI机制，查找"META-INF/service/org.apache.rocketmq.broker.transaction.TransactionalMessageService"文件里面的SPI实现
        //事务消息服务
        this.transactionalMessageService = ServiceProvider.loadClass(ServiceProvider.TRANSACTION_SERVICE_ID, TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            //如果沒有通过SPI指定具体的实现，那么使用默认实现，TransactionalMessageServiceImpl
            this.transactionalMessageService = new TransactionalMessageServiceImpl(new TransactionalMessageBridge(this, this.getMessageStore()));
            log.warn("Load default transaction message hook service: {}", TransactionalMessageServiceImpl.class.getSimpleName());
        }
        //基于Java的SPI机制，查找"META-INF/service/org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener"文件里面的SPI实现
        //事务消息回查服务监听器
        this.transactionalMessageCheckListener = ServiceProvider.loadClass(ServiceProvider.TRANSACTION_LISTENER_ID, AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            //如果沒有通过SPI指定具体的实现，那么使用默认实现，DefaultTransactionalMessageCheckListener
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            log.warn("Load default discard message hook service: {}", DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        this.transactionalMessageCheckListener.setBrokerController(this);
        //创建TransactionalMessageCheckService服务，该服务内部有一个线程
        //会定时每一分钟(可通过在broker.conf文件设置transactionCheckInterval属性更改)触发事务检查的逻辑，内部调用TransactionalMessageService#check方法
        //默认情况下，6秒以上没commit/rollback的事务消息才会触发事务回查，而如果回查次数超过15次则丢弃事务
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(this);
    }

    /**
     * BrokerController的方法
     * 初始化ACL权限校验器
     */
    private void initialAcl() {
        //校验是否开启了ACL，默认false，所以直接返回了
        if (!this.brokerConfig.isAclEnable()) {
            log.info("The broker dose not enable acl");
            return;
        }
        //如果开启了ACL，则首先通过SPI机制获取AccessValidator
        List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
        if (accessValidators == null || accessValidators.isEmpty()) {
            log.info("The broker dose not load the AccessValidator");
            return;
        }
        //将校验器存入accessValidatorMap，并且注册到RPCHook中，在请求之前会执行校验。
        for (AccessValidator accessValidator : accessValidators) {
            final AccessValidator validator = accessValidator;
            accessValidatorMap.put(validator.getClass(), validator);
            this.registerServerRPCHook(new RPCHook() {

                @Override
                public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                    //Do not catch the exception
                    //在执行请求之前会进行校验
                    validator.validate(validator.parse(request, remoteAddr));
                }

                @Override
                public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                }
            });
        }
    }

    /**
     * BrokerController的方法
     * 初始化RpcHook服务
     */
    private void initialRpcHooks() {
        //通过SPI机制获取RPCHook的实现
        List<RPCHook> rpcHooks = ServiceProvider.load(ServiceProvider.RPC_HOOK_ID, RPCHook.class);
        //如果没有配置RpcHook，那么直接返回
        if (rpcHooks == null || rpcHooks.isEmpty()) {
            return;
        }
        //遍历并且注册所有的RpcHook
        for (RPCHook rpcHook : rpcHooks) {
            this.registerServerRPCHook(rpcHook);
        }
    }


    /**
     * BrokerController的方法
     * 注册netty消息处理器
     */
    public void registerProcessor() {
        /**
         * 发送消息处理器
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

        //对于发送类型的请求，使用发送消息处理器sendProcessor来处理
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        /**
         * 拉取消息处理器
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /**
         * 回复消息处理器
         */
        ReplyMessageProcessor replyMessageProcessor = new ReplyMessageProcessor(this);
        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.heartbeatExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.heartbeatExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

        /**
         * ConsumerManageProcessor
         */
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.endTransactionExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.endTransactionExecutor);

        /**
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    public void protectBroker() {
        if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            final Iterator<Map.Entry<String, MomentStatsItem>> it = this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<String, MomentStatsItem> next = it.next();
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.headSlowTimeMills(this.queryThreadPoolQueue);
    }

    public long headSlowTimeMills4EndTransactionThreadPoolQueue() {
        return this.headSlowTimeMills(this.endTransactionThreadPoolQueue);
    }

    public void printWaterMark() {
        LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", this.queryThreadPoolQueue.size(), headSlowTimeMills4QueryThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}", this.endTransactionThreadPoolQueue.size(), headSlowTimeMills4EndTransactionThreadPoolQueue());
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("Slave fall behind master: {} bytes", diff);
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.putMessageFutureExecutor != null) {
            this.putMessageFutureExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.replyMessageExecutor != null) {
            this.replyMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }

        if (this.clientManageExecutor != null) {
            this.clientManageExecutor.shutdown();
        }

        if (this.queryMessageExecutor != null) {
            this.queryMessageExecutor.shutdown();
        }

        if (this.consumerManageExecutor != null) {
            this.consumerManageExecutor.shutdown();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

        if (this.endTransactionExecutor != null) {
            this.endTransactionExecutor.shutdown();
        }
    }

    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    /**
     * BrokerController的方法
     * 启动BrokerController
     */
    public void start() throws Exception {
        //启动消息存储服务
        if (this.messageStore != null) {
            this.messageStore.start();
        }
        //启动netty远程服务
        if (this.remotingServer != null) {
            this.remotingServer.start();
        }
        //启动快速netty远程服务
        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }
        //文件监听器启动
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }
        //broker对外api启动
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }
        //长轮询拉取消息挂起服务启动
        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }
        //客户端连接心跳服务启动
        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }
        //过滤服务管理器启动
        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }
        //如果没有开启DLeger的相关设置，默认没有启动
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            //如果不是SLAVE，那么启动transactionalMessageCheckService事务消息检查服务
            startProcessorByHa(messageStoreConfig.getBrokerRole());
            //如果是SLAVE，那么启动定时任务每隔10s与master机器同步数据，采用slave主动拉取的方法
            //同步的内容包括topic配置，消费者消费位移、延迟消息偏移量、订阅组信息等
            handleSlaveSynchronize(messageStoreConfig.getBrokerRole());
            /*
             * 强制注册当前broker信息到所有的nameserver
             */
            this.registerBrokerAll(true, false, true);
        }
        //启动定时任务，默认情况下每隔30s向nameServer进行一次注册，
        //时间间隔可以配置registerNameServerPeriod属性，允许的值是在1万到6万毫秒之间。
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    //定时发送心跳包并上报数据
                    BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);
        //broker相关统计服务启动
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }
        //broker快速失败服务启动
        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        TopicConfig registerTopicConfig = topicConfig;
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            registerTopicConfig =
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                            this.brokerConfig.getBrokerPermission());
        }

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
        topicConfigTable.put(topicConfig.getTopicName(), registerTopicConfig);
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    /**
     * BrokerController的方法
     * 注册Broker信息到NameServer，发送心跳包
     *
     * @param checkOrderConfig 是否检测顺序topic
     * @param oneway           是否是单向
     * @param forceRegister    是否强制注册
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {
        //在此前的topicConfigManager.load()方法中已经加载了所有topic信息，topic配置文件加载路径为{user.home}/store/config/topics.json
        //该方法根据TopicConfigManager中的topic信息构建topic信息的传输协议对象，除了包括topicConfigTable之外，还有dataVersion
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        //如果当前broker权限不支持写或者读
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                //那么重新配置topic权限
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                                this.brokerConfig.getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }
        /*
         * 如果forceRegister为true，表示强制注册，或者如果当前broker应该注册，那么向nameServer进行注册
         */
        if (forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getRegisterBrokerTimeoutMills())) {
            /*
             * 执行注册
             */
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    /**
     * BrokerController的方法
     *
     * @param checkOrderConfig   是否检测顺序topic
     * @param oneway             是否是单向
     * @param topicConfigWrapper topic信息的传输协议包装对象
     */
    private void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
                                     TopicConfigSerializeWrapper topicConfigWrapper) {
        /*
         * 执行注册，broker作为客户端向所有的nameserver发起注册请求
         */
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.getHAServerAddr(),
                //包含了携带topic信息的topicConfigTable，以及版本信息的dataVersion
                //这两个信息保存在持久化文件topics.json中
                topicConfigWrapper,
                this.filterServerManager.buildNewFilterServerList(),
                oneway,
                this.brokerConfig.getRegisterBrokerTimeoutMills(),
                this.brokerConfig.isCompressedRegister());
        /*
         * 对执行结果进行处理，选择抵押给调用的结果作为默认数据设置
         */
        if (registerBrokerResultList.size() > 0) {
            RegisterBrokerResult registerBrokerResult = registerBrokerResultList.get(0);
            if (registerBrokerResult != null) {
                if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                    this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                }

                this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
            }
        }
    }

    /**
     * BrokerController的方法
     * <p>
     * broker是否需要向nemeserver中注册
     *
     * @param clusterName  集群名
     * @param brokerAddr   broker地址
     * @param brokerName   broker名字
     * @param brokerId     brkerid
     * @param timeoutMills 超时时间
     * @return broker是否需要向nemeserver中注册
     */
    private boolean needRegister(final String clusterName,
                                 final String brokerAddr,
                                 final String brokerName,
                                 final long brokerId,
                                 final int timeoutMills) {
        //根据TopicConfigManager中的topic信息构建topic信息的传输协议对象，
        //在此前的topicConfigManager.load()方法中已经加载了所有topic信息，topic配置文件加载路径为{user.home}/store/config/topics.json
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        /*
         * 获取所有nameServer的DataVersion数据，一一对比自身数据是否一致，如果有一个nameserver的DataVersion数据版本不一致则重新注册
         */
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills);
        boolean needRegister = false;
        //如果和一个nameServer的数据版本不一致，则需要重新注册
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
        this.fastRemotingServer.registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
        return heartbeatThreadPoolQueue;
    }

    public TransactionalMessageCheckService getTransactionalMessageCheckService() {
        return transactionalMessageCheckService;
    }

    public void setTransactionalMessageCheckService(
            TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
        return transactionalMessageCheckListener;
    }

    public void setTransactionalMessageCheckListener(
            AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }


    public BlockingQueue<Runnable> getEndTransactionThreadPoolQueue() {
        return endTransactionThreadPoolQueue;

    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return accessValidatorMap;
    }

    private void handleSlaveSynchronize(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            this.slaveSynchronize.setMasterAddr(null);
            slaveSyncFuture = this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.slaveSynchronize.syncAll();
                    } catch (Throwable e) {
                        log.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                    }
                }
            }, 1000 * 3, 1000 * 10, TimeUnit.MILLISECONDS);
        } else {
            //handle the slave synchronise
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            this.slaveSynchronize.setMasterAddr(null);
        }
    }

    public void changeToSlave(int brokerId) {
        log.info("Begin to change to slave brokerName={} brokerId={}", brokerConfig.getBrokerName(), brokerId);

        //change the role
        brokerConfig.setBrokerId(brokerId == 0 ? 1 : brokerId); //TO DO check
        messageStoreConfig.setBrokerRole(BrokerRole.SLAVE);

        //handle the scheduled service
        try {
            this.messageStore.handleScheduleMessageService(BrokerRole.SLAVE);
        } catch (Throwable t) {
            log.error("[MONITOR] handleScheduleMessageService failed when changing to slave", t);
        }

        //handle the transactional service
        try {
            this.shutdownProcessorByHa();
        } catch (Throwable t) {
            log.error("[MONITOR] shutdownProcessorByHa failed when changing to slave", t);
        }

        //handle the slave synchronise
        handleSlaveSynchronize(BrokerRole.SLAVE);

        try {
            this.registerBrokerAll(true, true, true);
        } catch (Throwable ignored) {

        }
        log.info("Finish to change to slave brokerName={} brokerId={}", brokerConfig.getBrokerName(), brokerId);
    }


    public void changeToMaster(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            return;
        }
        log.info("Begin to change to master brokerName={}", brokerConfig.getBrokerName());

        //handle the slave synchronise
        handleSlaveSynchronize(role);

        //handle the scheduled service
        try {
            this.messageStore.handleScheduleMessageService(role);
        } catch (Throwable t) {
            log.error("[MONITOR] handleScheduleMessageService failed when changing to master", t);
        }

        //handle the transactional service
        try {
            this.startProcessorByHa(BrokerRole.SYNC_MASTER);
        } catch (Throwable t) {
            log.error("[MONITOR] startProcessorByHa failed when changing to master", t);
        }

        //if the operations above are totally successful, we change to master
        brokerConfig.setBrokerId(0); //TO DO check
        messageStoreConfig.setBrokerRole(role);

        try {
            this.registerBrokerAll(true, true, true);
        } catch (Throwable ignored) {

        }
        log.info("Finish to change to master brokerName={}", brokerConfig.getBrokerName());
    }

    private void startProcessorByHa(BrokerRole role) {
        if (BrokerRole.SLAVE != role) {
            if (this.transactionalMessageCheckService != null) {
                this.transactionalMessageCheckService.start();
            }
        }
    }

    private void shutdownProcessorByHa() {
        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(true);
        }
    }

    public ExecutorService getPutMessageFutureExecutor() {
        return putMessageFutureExecutor;
    }
}
