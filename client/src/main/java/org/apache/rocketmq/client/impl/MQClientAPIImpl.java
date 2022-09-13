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
package org.apache.rocketmq.client.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateAccessConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteAccessConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerAclConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerClusterAclConfigResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateGlobalWhiteAddrsConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterMessageFilterClassRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import com.alibaba.fastjson.JSON;

public class MQClientAPIImpl {

    private final static InternalLogger log = ClientLogger.getLog();
    private static boolean sendSmartMsg =
            Boolean.parseBoolean(System.getProperty("org.apache.rocketmq.client.sendSmartMsg", "true"));

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    }

    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private String nameSrvAddr = null;
    private ClientConfig clientConfig;

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
                           final ClientRemotingProcessor clientRemotingProcessor,
                           RPCHook rpcHook, final ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        topAddressing = new TopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
        this.clientRemotingProcessor = clientRemotingProcessor;

        this.remotingClient.registerRPCHook(rpcHook);
        this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this.clientRemotingProcessor, null);
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> list = Arrays.asList(addrArray);
        this.remotingClient.updateNameServerAddressList(list);
    }

    /**
     * MQClientAPIImpl的方法
     */
    public void start() {
        //调用NettyRemotingClient#start
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    public void createSubscriptionGroup(final String addr, final SubscriptionGroupConfig config,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

        byte[] body = RemotingSerializable.encode(config);
        request.setBody(body);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());

    }

    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
                            final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void createPlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        CreateAccessConfigRequestHeader requestHeader = new CreateAccessConfigRequestHeader();
        requestHeader.setAccessKey(plainAccessConfig.getAccessKey());
        requestHeader.setSecretKey(plainAccessConfig.getSecretKey());
        requestHeader.setAdmin(plainAccessConfig.isAdmin());
        requestHeader.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
        requestHeader.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
        requestHeader.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        requestHeader.setTopicPerms(UtilAll.join(plainAccessConfig.getTopicPerms(), ","));
        requestHeader.setGroupPerms(UtilAll.join(plainAccessConfig.getGroupPerms(), ","));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteAccessConfig(final String addr, final String accessKey, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        DeleteAccessConfigRequestHeader requestHeader = new DeleteAccessConfigRequestHeader();
        requestHeader.setAccessKey(accessKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void updateGlobalWhiteAddrsConfig(final String addr, final String globalWhiteAddrs, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        UpdateGlobalWhiteAddrsConfigRequestHeader requestHeader = new UpdateGlobalWhiteAddrsConfigRequestHeader();
        requestHeader.setGlobalWhiteAddrs(globalWhiteAddrs);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ClusterAclVersionInfo getBrokerClusterAclInfo(final String addr,
                                                         final long timeoutMillis) throws RemotingCommandException, InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetBrokerAclConfigResponseHeader responseHeader =
                        (GetBrokerAclConfigResponseHeader) response.decodeCommandCustomHeader(GetBrokerAclConfigResponseHeader.class);

                ClusterAclVersionInfo clusterAclVersionInfo = new ClusterAclVersionInfo();
                clusterAclVersionInfo.setClusterName(responseHeader.getClusterName());
                clusterAclVersionInfo.setBrokerName(responseHeader.getBrokerName());
                clusterAclVersionInfo.setBrokerAddr(responseHeader.getBrokerAddr());
                clusterAclVersionInfo.setAclConfigDataVersion(DataVersion.fromJson(responseHeader.getVersion(), DataVersion.class));
                HashMap<String, Object> dataVersionMap = JSON.parseObject(responseHeader.getAllAclFileVersion(), HashMap.class);
                Map<String, DataVersion> allAclConfigDataVersion = new HashMap<String, DataVersion>();
                for (Map.Entry<String, Object> entry : dataVersionMap.entrySet()) {
                    allAclConfigDataVersion.put(entry.getKey(), DataVersion.fromJson(JSON.toJSONString(entry.getValue()), DataVersion.class));
                }
                clusterAclVersionInfo.setAllAclConfigDataVersion(allAclConfigDataVersion);
                return clusterAclVersionInfo;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

    }

    public AclConfig getBrokerClusterConfig(final String addr,
                                            final long timeoutMillis) throws RemotingCommandException, InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetBrokerClusterAclConfigResponseBody body =
                            GetBrokerClusterAclConfigResponseBody.decode(response.getBody(), GetBrokerClusterAclConfigResponseBody.class);
                    AclConfig aclConfig = new AclConfig();
                    aclConfig.setGlobalWhiteAddrs(body.getGlobalWhiteAddrs());
                    aclConfig.setPlainAccessConfigs(body.getPlainAccessConfigs());
                    return aclConfig;
                }
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

    }

    public SendResult sendMessage(
            final String addr,
            final String brokerName,
            final Message msg,
            final SendMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
    }

    /**
     * MQClientAPIImpl的方法
     * 同步、异步、单向消息的最终发送消息的方法
     *
     * @param addr                     brokerAddr
     * @param brokerName               brokerName
     * @param msg                      msg
     * @param requestHeader            requestHeader
     * @param timeoutMillis            剩余超时时间
     * @param communicationMode        发送模式
     * @param sendCallback             发送回调函数
     * @param topicPublishInfo         topic信息
     * @param instance                 MQClientInstance
     * @param retryTimesWhenSendFailed 异步发送失败时的重试次数，默认2
     * @param context                  发送消息上下文
     * @param producer                 DefaultMQProducerImpl
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public SendResult sendMessage(
            final String addr,
            final String brokerName,
            final Message msg,
            final SendMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance instance,
            final int retryTimesWhenSendFailed,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        RemotingCommand request = null;
        //消息类型
        String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
        //是否时重试消息
        boolean isReply = msgType != null && msgType.equals(MixAll.REPLY_MESSAGE_FLAG);
        /*
         * 1 构建发送消息命令对象RemotingCommand，更换轻量级消息头
         *
         * sendSmartMsg表示是否使用更轻量级的消息头SendMessageRequestHeaderV2
         * 相比于requestHeader，其field 全为 a,b,c,d 等短变量名，可以加快FastJson反序列化过程。
         */
        if (isReply) {
            //sendSmartMsg默认为true
            if (sendSmartMsg) {
                //构建轻量级消息头
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                //创建发送消息命令对象，RequestCode为SEND_REPLY_MESSAGE_V2
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
            } else {
                //创建发送消息命令对象，RequestCode为SEND_REPLY_MESSAGE
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
            }
        } else {
            if (sendSmartMsg || msg instanceof MessageBatch) {
                //构建轻量级消息头
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                //如果是批量消息，则RequestCode为SEND_BATCH_MESSAGE，否则RequestCode为SEND_MESSAGE_V2
                request = RemotingCommand.createRequestCommand(msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
            } else {
                //创建发送消息命令对象，RequestCode为SEND_MESSAGE
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }
        }
        //设置body
        request.setBody(msg.getBody());
        /*
         * 2 根据发送模式执行不同的发送逻辑
         */
        switch (communicationMode) {
            case ONEWAY:
                /*
                 * 单向发送，调用该方法之后不接收返回值，直接返回null
                 */
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case ASYNC:
                //异步消息重试计数器
                final AtomicInteger times = new AtomicInteger();
                //计算是否超时，如果超时则不再发送
                long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeAsync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                /*
                 * 异步发送，调用该方法之后不接收返回值，直接返回null
                 */
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, context, producer);
                return null;
            case SYNC:
                //计算是否超时，如果超时则不再发送
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                /*
                 * 同步发送，调用该方法之后阻塞直至收到返回值
                 */
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
            default:
                assert false;
                break;
        }

        return null;
    }

    /**
     * MQClientAPIImpl的方法
     */
    private SendResult sendMessageSync(
            final String addr,
            final String brokerName,
            final Message msg,
            final long timeoutMillis,
            final RemotingCommand request
    ) throws RemotingException, MQBrokerException, InterruptedException {
        /*
         * 发送同步消息
         */
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        /*
         * 处理响应结果
         */
        return this.processSendResponse(brokerName, msg, response, addr);
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 异步发送消息的方法，包含重试的逻辑
     *
     * @param retryTimesWhenSendFailed 异步发送失败重试次数
     * @param times                    重试计数器
     */
    private void sendMessageAsync(
            final String addr,
            final String brokerName,
            final Message msg,
            final long timeoutMillis,
            final RemotingCommand request,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance instance,
            final int retryTimesWhenSendFailed,
            final AtomicInteger times,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer
    ) throws InterruptedException, RemotingException {
        //起始时间
        final long beginStartTime = System.currentTimeMillis();
        try {
            /*
             * 发送异步消息，设置一个InvokeCallback回调对象
             *
             * InvokeCallback#operationComplete方法将会在得到结果之后进行回调
             */
            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
                /**
                 * 异步执行的回调方法
                 */
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    //花费的时间
                    long cost = System.currentTimeMillis() - beginStartTime;
                    RemotingCommand response = responseFuture.getResponseCommand();
                    //如果没有设置回调函数
                    if (null == sendCallback && response != null) {

                        try {
                            //调用processSendResponse方法处理响应结果
                            SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
                            if (context != null && sendResult != null) {
                                context.setSendResult(sendResult);
                                context.getProducer().executeSendMessageHookAfter(context);
                            }
                        } catch (Throwable e) {
                        }
                        //更新本地错误表缓存数据，用于延迟时间的故障转移的功能
                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                        return;
                    }
                    //如果存在响应结果
                    if (response != null) {
                        try {
                            //调用processSendResponse方法处理响应结果
                            SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
                            assert sendResult != null;
                            if (context != null) {
                                context.setSendResult(sendResult);
                                //执行消息发送之后的钩子函数，即SendMessageHook#sendMessageAfter方法
                                context.getProducer().executeSendMessageHookAfter(context);
                            }

                            try {
                                //响应成功时调用，执行sendCallback的onSuccess方法
                                //这里的sendCallback就是发送消息时传入的回调函数
                                sendCallback.onSuccess(sendResult);
                            } catch (Throwable e) {
                            }
                            //更新本地错误表缓存数据，用于延迟时间的故障转移的功能
                            producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                        } catch (Exception e) {
                            producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
                            //异常处理，不需要重试
                            onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, e, context, false, producer);
                        }
                    } else {
                        //更新本地错误表缓存数据，用于延迟时间的故障转移的功能
                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
                        //异常处理，需要重试
                        //发送失败
                        if (!responseFuture.isSendRequestOK()) {
                            MQClientException ex = new MQClientException("send request failed", responseFuture.getCause());
                            onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                        } else if (responseFuture.isTimeout()) {
                            //超时
                            MQClientException ex = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                                    responseFuture.getCause());
                            onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                        } else {
                            MQClientException ex = new MQClientException("unknow reseaon", responseFuture.getCause());
                            onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                        }
                    }
                }
            });
        } catch (Exception ex) {
            long cost = System.currentTimeMillis() - beginStartTime;
            //更新本地错误表缓存数据，用于延迟时间的故障转移的功能
            producer.updateFaultItem(brokerName, cost, true);
            //异常处理
            onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                    retryTimesWhenSendFailed, times, ex, context, true, producer);
        }
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 异步发送消息的异常处理的方法，包含重试的逻辑
     *
     * @param e          抛出的异常
     * @param timesTotal 异步发送失败重试次数
     * @param curTimes   重试计数器
     * @param needRetry  是否需要重试
     */
    private void onExceptionImpl(final String brokerName,
                                 final Message msg,
                                 final long timeoutMillis,
                                 final RemotingCommand request,
                                 final SendCallback sendCallback,
                                 final TopicPublishInfo topicPublishInfo,
                                 final MQClientInstance instance,
                                 final int timesTotal,
                                 final AtomicInteger curTimes,
                                 final Exception e,
                                 final SendMessageContext context,
                                 final boolean needRetry,
                                 final DefaultMQProducerImpl producer
    ) {
        //重试次数加1并获取
        int tmp = curTimes.incrementAndGet();
        /*
         * 如果需要重试，并且本次重试次数小于等于总次数
         */
        if (needRetry && tmp <= timesTotal) {
            //保存brokerName，异步重试将会发送到相同的broker
            String retryBrokerName = brokerName;//by default, it will send to the same broker
            if (topicPublishInfo != null) { //select one message queue accordingly, in order to determine which broker to send
                //选择一个消息队列
                MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName);
                retryBrokerName = mqChosen.getBrokerName();
            }
            String addr = instance.findBrokerAddressInPublish(retryBrokerName);
            log.warn(String.format("async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}", tmp, msg.getTopic(), addr,
                    retryBrokerName), e);
            try {
                //设置请求id，通过id可以获取请求结果
                request.setOpaque(RemotingCommand.createNewRequestId());
                //调用sendMessageAsync再次发送异步消息
                sendMessageAsync(addr, retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                        timesTotal, curTimes, context, producer);
            } catch (InterruptedException e1) {
                //继续重试
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
            } catch (RemotingTooMuchRequestException e1) {
                //继续重试
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
            } catch (RemotingException e1) {
                //更新异常表
                producer.updateFaultItem(brokerName, 3000, true);
                //继续重试
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, true, producer);
            }
        } else {
            //如果本次重试次数大于总次数
            if (context != null) {
                context.setException(e);
                context.getProducer().executeSendMessageHookAfter(context);
            }

            try {
                //最后执行sendCallback的onException方法
                sendCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 处理响应结果
     */
    private SendResult processSendResponse(
            final String brokerName,
            final Message msg,
            final RemotingCommand response,
            final String addr
    ) throws MQBrokerException, RemotingCommandException {
        SendStatus sendStatus;
        //根据responseCode设置对应的SendStatus
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            }
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            }
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            }
            case ResponseCode.SUCCESS: {
                sendStatus = SendStatus.SEND_OK;
                break;
            }
            default: {
                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
            }
        }
        //获取响应头
        SendMessageResponseHeader responseHeader =
                (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

        //If namespace not null , reset Topic without namespace.
        //如果命名空间不为null，重置不带命名空间的topic
        String topic = msg.getTopic();
        if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
            topic = NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace());
        }
        //新建一个MessageQueue，设置属性
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());
        //获取客户端生成的uniqId
        String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
        if (msg instanceof MessageBatch) {
            StringBuilder sb = new StringBuilder();
            for (Message message : (MessageBatch) msg) {
                sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(message));
            }
            uniqMsgId = sb.toString();
        }
        //新建SendResult，即最终返回的对象，设置客户端生成的uniqMsgId为msgId属性，设置broker生成的MsgId为offsetMsgId属性
        SendResult sendResult = new SendResult(sendStatus,
                uniqMsgId,
                responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
        //设置事务id
        sendResult.setTransactionId(responseHeader.getTransactionId());
        //设置regionId和traceOn
        String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
        String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
        if (regionId == null || regionId.isEmpty()) {
            regionId = MixAll.DEFAULT_TRACE_REGION_ID;
        }
        if (traceOn != null && traceOn.equals("false")) {
            sendResult.setTraceOn(false);
        } else {
            sendResult.setTraceOn(true);
        }
        sendResult.setRegionId(regionId);
        return sendResult;
    }

    /**
     * MQClientAPIImpl的方法
     *
     * @param addr              broker地址
     * @param requestHeader     请求头
     * @param timeoutMillis     消费者消息拉取超时时间，默认30s
     * @param communicationMode 消息拉取模式，默认为异步拉取
     * @param pullCallback      拉取到消息之后调用的回调函数
     * @return 拉取结果
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pullMessage(
            final String addr,
            final PullMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws RemotingException, MQBrokerException, InterruptedException {
        //构建请求命令对象，请求Code为PULL_MESSAGE
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);

        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                //push模式默认异步拉取消息
                this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(addr, request, timeoutMillis);
            default:
                assert false;
                break;
        }

        return null;
    }

    /**
     * MQClientAPIImpl的方法
     * 异步的拉取消息，并且触发回调函数
     *
     * @param addr          broker地址
     * @param request       请求命令对象
     * @param timeoutMillis 消费者消息拉取超时时间，默认30s
     * @param pullCallback  拉取到消息之后调用的回调函数
     * @throws RemotingException
     * @throws InterruptedException
     */
    private void pullMessageAsync(
            final String addr,
            final RemotingCommand request,
            final long timeoutMillis,
            final PullCallback pullCallback
    ) throws RemotingException, InterruptedException {
        /*
         * 基于netty给broker发送异步消息，设置一个InvokeCallback回调对象
         *
         * InvokeCallback#operationComplete方法将会在得到结果之后进行回调，内部调用pullCallback的回调方法
         */
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
            /**
             * 异步执行的回调方法
             */
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                //返回命令对象
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        //解析响应获取结果
                        PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response, addr);
                        assert pullResult != null;
                        //如果解析到了结果，那么调用pullCallback#onSuccess方法处理
                        pullCallback.onSuccess(pullResult);
                    } catch (Exception e) {
                        //出现异常则调用pullCallback#onException方法处理异常
                        pullCallback.onException(e);
                    }
                } else {
                    //没有结果，都调用onException方法处理异常
                    if (!responseFuture.isSendRequestOK()) {
                        //发送失败
                        pullCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        //超时
                        pullCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        pullCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    private PullResult pullMessageSync(
            final String addr,
            final RemotingCommand request,
            final long timeoutMillis
    ) throws RemotingException, InterruptedException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response, addr);
    }

    /**
     * MQClientAPIImpl的方法
     * 处理response获取PullResult
     */
    private PullResult processPullResponse(
            final RemotingCommand response,
            final String addr) throws MQBrokerException, RemotingCommandException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        //设置结果状态码
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;

            default:
                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
        //解析响应头
        PullMessageResponseHeader responseHeader =
                (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);
        //根据响应的数据创建PullResultExt对象返回，此时拉取到的消息还是一个字节数组
        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
                responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody());
    }

    public MessageExt viewMessage(final String addr, final long phyoffset, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(phyoffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                //If namespace not null , reset Topic without namespace.
                if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
                    messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.clientConfig.getNamespace()));
                }
                return messageExt;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
                             final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                SearchOffsetResponseHeader responseHeader =
                        (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long getMaxOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMaxOffsetResponseHeader responseHeader =
                        (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * MQClientAPIImpl的方法
     * 根据brokerAddr和group 得到消费者客户端id列表
     */
    public List<String> getConsumerIdListByGroup(
            final String addr,
            final String consumerGroup,
            final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            MQBrokerException, InterruptedException {
        //构建请求头
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        //构建请求命令对象，Code为GET_CONSUMER_LIST_BY_GROUP
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        //发起同步调用
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    //响应解码
                    GetConsumerListByGroupResponseBody body =
                            GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
                    //返回客户端id集合
                    return body.getConsumerIdList();
                }
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long getMinOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMinOffsetResponseHeader responseHeader =
                        (GetMinOffsetResponseHeader) response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long getEarliestMsgStoretime(final String addr, final String topic, final int queueId,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetEarliestMsgStoretimeResponseHeader responseHeader =
                        (GetEarliestMsgStoretimeResponseHeader) response.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);

                return responseHeader.getTimestamp();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long queryConsumerOffset(
            final String addr,
            final QueryConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumerOffsetResponseHeader responseHeader =
                        (QueryConsumerOffsetResponseHeader) response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateConsumerOffset(
            final String addr,
            final UpdateConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 更新消费偏移量单向情求
     *
     * @param addr          broker地址
     * @param requestHeader 请求头
     * @param timeoutMillis 超时时间
     */
    public void updateConsumerOffsetOneway(
            final String addr,
            final UpdateConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException,
            InterruptedException {
        //请求Code为UPDATE_CONSUMER_OFFSET
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        //单向调用，可以走vip通道
        this.remotingClient.invokeOneway(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
    }

    /**
     * MQClientAPIImpl的方法
     * 发送心跳请求
     *
     * @param addr          broker地址
     * @param heartbeatData 心跳数据包
     * @param timeoutMillis 超时时间
     * @return 版本
     */
    public int sendHearbeat(
            final String addr,
            final HeartbeatData heartbeatData,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        //构建心跳RemotingCommand，Code为HEART_BEAT
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(clientConfig.getLanguage());
        request.setBody(heartbeatData.encode());
        //同步调用等待返回结果
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return response.getVersion();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void unregisterClient(
            final String addr,
            final String clientID,
            final String producerGroup,
            final String consumerGroup,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * MQClientAPIImpl的方法
     *
     * @param addr          broker地址
     * @param requestHeader 请求头
     * @param remark        本地事务执行抛出的异常
     * @param timeoutMillis 超时时间
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public void endTransactionOneway(
            final String addr,
            final EndTransactionRequestHeader requestHeader,
            final String remark,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        //请求Code为END_TRANSACTION
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

        request.setRemark(remark);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public void queryMessage(
            final String addr,
            final QueryMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final InvokeCallback invokeCallback,
            final Boolean isUnqiueKey
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.toString());
        this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis,
                invokeCallback);
    }

    public boolean registerClient(final String addr, final HeartbeatData heartbeat, final long timeoutMillis)
            throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);

        request.setBody(heartbeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        return response.getCode() == ResponseCode.SUCCESS;
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 发送消费失败的消息到broker
     *
     * @param addr                 broker地址
     * @param msg                  要发回的消息
     * @param consumerGroup        消费者组
     * @param delayLevel           延迟等级
     * @param timeoutMillis        超时时间
     * @param maxConsumeRetryTimes 最大重试次数，超过发往死信队列
     */
    public void consumerSendMessageBack(
            final String addr,
            final MessageExt msg,
            final String consumerGroup,
            final int delayLevel,
            final long timeoutMillis,
            final int maxConsumeRetryTimes
    ) throws RemotingException, MQBrokerException, InterruptedException {
        //构建请求头
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        //Code为CONSUMER_SEND_MSG_BACK
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());
        requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);
        //同步调用
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 向broker发送同步请求，Code为LOCK_BATCH_MQ，请求批量锁定消息队列，返回锁住的mq集合
     *
     * @param addr          broker地址
     * @param requestBody   请求体
     * @param timeoutMillis 超时时间
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public Set<MessageQueue> lockBatchMQ(
            final String addr,
            final LockBatchRequestBody requestBody,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        //Code为LOCK_BATCH_MQ
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        //同步请求，调用brokerVIPChannel判断是否开启vip通道，如果开启了，那么将brokerAddr的port – 2，因为vip通道的端口为普通端口 – 2。
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                //解码
                LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
                Set<MessageQueue> messageQueues = responseBody.getLockOKMQSet();
                return messageQueues;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void unlockBatchMQ(
            final String addr,
            final UnlockBatchRequestBody requestBody,
            final long timeoutMillis,
            final boolean oneway
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());

        if (oneway) {
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        } else {
            RemotingCommand response = this.remotingClient
                    .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
    }

    public TopicStatsTable getTopicStatsInfo(final String addr, final String topic,
                                             final long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                TopicStatsTable topicStatsTable = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
                return topicStatsTable;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQBrokerException {
        return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final String topic,
                                        final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQBrokerException {
        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
                return consumeStats;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ProducerConnection getProducerConnectionList(final String addr, final String producerGroup,
                                                        final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ProducerConnection.decode(response.getBody(), ProducerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumerConnection getConsumerConnectionList(final String addr, final String consumerGroup,
                                                        final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public KVTable getBrokerRuntimeInfo(final String addr, final long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return KVTable.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateBrokerConfig(final String addr, final Properties properties, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient
                    .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
    }

    public Properties getBrokerConfig(final String addr, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET));
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ClusterInfo getBrokerClusterInfo(
            final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, false);
    }

    /**
     * MQClientAPIImpl的方法
     * <p>
     * 从nameServer获取当前指定topic的路由信息
     *
     * @param topic         指定topic名字
     * @param timeoutMillis 超时时间3s
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        //允许topic不存在
        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
                                                          boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        //构建请求头
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        //topic设置到请求头中
        requestHeader.setTopic(topic);
        //获取请求命令对象，请求的Code为GET_ROUTEINFO_BY_TOPIC，105
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);
        //同步远程调用，请求nameServer服务，注意这里的addr参数为nll，即producer会随机连接一个nameServer
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }

                break;
            }
            case ResponseCode.SUCCESS: {
                //如果响应成功，那么对响应体进行解码
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicListFromNameServer(final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicList.decode(body, TopicList.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName,
                                     final long timeoutMillis) throws RemotingCommandException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                WipeWritePermOfBrokerResponseHeader responseHeader =
                        (WipeWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
                return responseHeader.getWipeTopicCount();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public int addWritePermOfBroker(final String nameSrvAddr, String brokerName, final long timeoutMillis)
            throws RemotingCommandException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        AddWritePermOfBrokerRequestHeader requestHeader = new AddWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(nameSrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                AddWritePermOfBrokerResponseHeader responseHeader =
                        (AddWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(AddWritePermOfBrokerResponseHeader.class);
                return responseHeader.getAddTopicCount();
            }
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInBroker(final String addr, final String topic, final long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(final String addr, final String topic, final long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {
        DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteSubscriptionGroup(final String addr, final String groupName, final boolean removeOffset,
                                        final long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {
        DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
        requestHeader.setGroupName(groupName);
        requestHeader.setRemoveOffset(removeOffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public String getKVConfigValue(final String namespace, final String key, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetKVConfigResponseHeader responseHeader =
                        (GetKVConfigResponseHeader) response.decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
                return responseHeader.getValue();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void putKVConfigValue(final String namespace, final String key, final String value, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }

            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public void deleteKVConfigValue(final String namespace, final String key, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }
            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public KVTable getKVListByNamespace(final String namespace, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
        requestHeader.setNamespace(namespace);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return KVTable.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
                                                             final long timestamp, final boolean isForce, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
                                                             final long timestamp, final boolean isForce, final long timeoutMillis, boolean isC)
            throws RemotingException, MQClientException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(isForce);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        if (isC) {
            request.setLanguage(LanguageCode.CPP);
        }

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
                    return body.getOffsetTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<String, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(final String addr, final String topic,
                                                                                final String group,
                                                                                final String clientAddr,
                                                                                final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setClientAddr(clientAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
                    return body.getConsumerTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public GroupList queryTopicConsumeByWho(final String addr, final String topic, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GroupList groupList = GroupList.decode(response.getBody(), GroupList.class);
                return groupList;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String addr, final String topic, final String group,
                                                    final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
                return consumeTimeSpanBody.getConsumeTimeSpanSet();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public TopicList getTopicsByCluster(final String cluster, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
        requestHeader.setCluster(cluster);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void registerMessageFilterClass(final String addr,
                                           final String consumerGroup,
                                           final String topic,
                                           final String className,
                                           final int classCRC,
                                           final byte[] classBody,
                                           final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException, MQBrokerException {
        RegisterMessageFilterClassRequestHeader requestHeader = new RegisterMessageFilterClassRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClassName(className);
        requestHeader.setTopic(topic);
        requestHeader.setClassCRC(classCRC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_MESSAGE_FILTER_CLASS, requestHeader);
        request.setBody(classBody);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public TopicList getSystemTopicList(
            final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (topicList.getTopicList() != null && !topicList.getTopicList().isEmpty()
                            && !UtilAll.isBlank(topicList.getBrokerAddr())) {
                        TopicList tmp = getSystemTopicListFromBroker(topicList.getBrokerAddr(), timeoutMillis);
                        if (tmp.getTopicList() != null && !tmp.getTopicList().isEmpty()) {
                            topicList.getTopicList().addAll(tmp.getTopicList());
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicListFromBroker(final String addr, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanExpiredConsumeQueue(final String addr,
                                            long timeoutMillis) throws MQClientException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanUnusedTopicByAddr(final String addr,
                                          long timeoutMillis) throws MQClientException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumerRunningInfo getConsumerRunningInfo(final String addr, String consumerGroup, String clientId,
                                                      boolean jstack,
                                                      final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setJstackEnable(jstack);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    ConsumerRunningInfo info = ConsumerRunningInfo.decode(body, ConsumerRunningInfo.class);
                    return info;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String addr,
                                                               String consumerGroup,
                                                               String clientId,
                                                               String msgId,
                                                               final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    ConsumeMessageDirectlyResult info = ConsumeMessageDirectlyResult.decode(body, ConsumeMessageDirectlyResult.class);
                    return info;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<Integer, Long> queryCorrectionOffset(final String addr, final String topic, final String group,
                                                    Set<String> filterGroup,
                                                    long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException {
        QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
        requestHeader.setCompareGroup(group);
        requestHeader.setTopic(topic);
        if (filterGroup != null) {
            StringBuilder sb = new StringBuilder();
            String splitor = "";
            for (String s : filterGroup) {
                sb.append(splitor).append(s);
                splitor = ",";
            }
            requestHeader.setFilterGroups(sb.toString());
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    QueryCorrectionOffsetBody body = QueryCorrectionOffsetBody.decode(response.getBody(), QueryCorrectionOffsetBody.class);
                    return body.getCorrectionOffsets();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getUnitTopicList(final boolean containRetry, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }

                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubTopicList(final boolean containRetry, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubUnUnitTopicList(final boolean containRetry, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void cloneGroupOffset(final String addr, final String srcGroup, final String destGroup, final String topic,
                                 final boolean isOffline,
                                 final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
        requestHeader.setSrcGroup(srcGroup);
        requestHeader.setDestGroup(destGroup);
        requestHeader.setTopic(topic);
        requestHeader.setOffline(isOffline);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName, String statsKey, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException {
        ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
        requestHeader.setStatsName(statsName);
        requestHeader.setStatsKey(statsKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);

        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return BrokerStatsData.decode(body, BrokerStatsData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Set<String> getClusterList(String topic,
                                      long timeoutMillis) throws MQClientException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return Collections.EMPTY_SET;
    }

    public ConsumeStatsList fetchConsumeStatsInBroker(String brokerAddr, boolean isOrder,
                                                      long timeoutMillis) throws MQClientException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        GetConsumeStatsInBrokerHeader requestHeader = new GetConsumeStatsInBrokerHeader();
        requestHeader.setIsOrder(isOrder);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return ConsumeStatsList.decode(body, ConsumeStatsList.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
                                                            long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public TopicConfigSerializeWrapper getAllTopicConfig(final String addr,
                                                         long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateNameServerConfig(final Properties properties, final List<String> nameServers, long timeoutMillis)
            throws UnsupportedEncodingException,
            MQBrokerException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQClientException {
        String str = MixAll.properties2String(properties);
        if (str == null || str.length() < 1) {
            return;
        }
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
                this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
        request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    break;
                }
                default:
                    errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public Map<String, Properties> getNameServerConfig(final List<String> nameServers, long timeoutMillis)
            throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQClientException, UnsupportedEncodingException {
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
                this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<String, Properties>(4);
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

            assert response != null;

            if (ResponseCode.SUCCESS == response.getCode()) {
                configMap.put(nameServer, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
            } else {
                throw new MQClientException(response.getCode(), response.getRemark());
            }
        }
        return configMap;
    }

    public QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr, final String topic,
                                                           final int queueId,
                                                           final long index, final int count, final String consumerGroup,
                                                           final long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {

        QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setIndex(index);
        requestHeader.setCount(count);
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;

        if (ResponseCode.SUCCESS == response.getCode()) {
            return QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void checkClientInBroker(final String brokerAddr, final String consumerGroup,
                                    final String clientId, final SubscriptionData subscriptionData,
                                    final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId(clientId);
        requestBody.setGroup(consumerGroup);
        requestBody.setSubscriptionData(subscriptionData);

        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;

        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
    }

    public boolean resumeCheckHalfMessage(final String addr, String msgId,
                                          final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        ResumeCheckHalfMessageRequestHeader requestHeader = new ResumeCheckHalfMessageRequestHeader();
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                log.error("Failed to resume half message check logic. Remark={}", response.getRemark());
                return false;
        }
    }
}
