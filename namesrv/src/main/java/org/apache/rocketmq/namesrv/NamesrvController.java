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
package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;


public class NamesrvController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "NSScheduledThread"));
    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;

    private RemotingServer remotingServer;

    private BrokerHousekeepingService brokerHousekeepingService;

    private ExecutorService remotingExecutor;

    private Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        //nameserver的配置
        this.namesrvConfig = namesrvConfig;
        //nameserver的netty服务的配置
        this.nettyServerConfig = nettyServerConfig;
        //kv配置管理器
        this.kvConfigManager = new KVConfigManager(this);
        //路由信息管理器
        this.routeInfoManager = new RouteInfoManager();
        //Broker连接的各种事件的处理服务，是处理Broker连接发生变化的服务
        //主要用于监听在Channel通道关闭事件触发时调用RouteInfoManager#onChannelDestroy清除路由信息
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        //配置类，并将namesrvConfig和nettyServerConfig的配置注册到内部的allConfigs集合中
        this.configuration = new Configuration(log, this.namesrvConfig, this.nettyServerConfig);
        //存储路径配置
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    public boolean initialize() {
        /*
         * 1 加载KV配置并存储到kvConfigManager内部的configTable属性中
         * KVConfig配置文件默认路径是 ${user.home}/namesrv/kvConfig.json
         */
        this.kvConfigManager.load();
        /*
         * 2 创建NameServer的netty远程服务
         * 设置了一个ChannelEventListener，为此前创建brokerHousekeepingService
         * remotingServer是一个基于Netty的用于NameServer与Broker、Consumer、Producer进行网络通信的服务端
         */
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        /*
         * 3 创建netty远程通信执行器线程池，用作默认的请求处理线程池，线程名以RemotingExecutorThread_为前缀
         */
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        /*
         * 4 注册默认请求处理器DefaultRequestProcessor
         * 将remotingExecutor绑定到DefaultRequestProcessor上，用作默认的请求处理线程池
         * DefaultRequestProcessor绑定到remotingServer的defaultRequestProcessor属性上
         */
        this.registerProcessor();
        /*
         * 5 启动一个定时任务
         * 首次启动延迟5秒执行，此后每隔10秒执行一次扫描无效的Broker，并清除Broker相关路由信息的任务
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                //扫描notActive的broker
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);
        /*
         * 6 启动一个定时任务
         * 首次启动延迟1分钟执行，此后每隔10分钟执行一次打印kv配置信息的任务
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                //打印kv配置信息
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);
        /*
         * Tls传输相关配置，通信安全的文件监听模块，用来观察网络加密配置文件的更改
         */
        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
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
                            }
                        });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                    this.remotingExecutor);
        } else {
            //将remotingExecutor绑定到DefaultRequestProcessor上，用作默认的请求处理线程池
            //将DefaultRequestProcessor绑定到remotingServer的defaultRequestProcessor属性上
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    /**
     * NamesrvController的方法
     */
    public void start() throws Exception {
        //调用remotingServer的启动方法
        this.remotingServer.start();

        if (this.fileWatchService != null) {
            //监听tts相关文件是否发生变化
            this.fileWatchService.start();
        }
    }

    public void shutdown() {
        //关闭nettyserver
        this.remotingServer.shutdown();
        //关闭线程池
        this.remotingExecutor.shutdown();
        //关闭定时任务
        this.scheduledExecutorService.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
