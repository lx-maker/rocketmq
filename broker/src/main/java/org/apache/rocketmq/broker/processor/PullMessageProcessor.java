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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionForRetryMessageFilter;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class PullMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private List<ConsumeMessageHook> consumeMessageHookList;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * PullMessageProcessor的方法
     * <p>
     * 处理PULL_MESSAGE拉取消息请求
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        //调用另一个processRequest方法，第三个参数broker是否支持挂起请求为true
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * PullMessageProcessor的方法
     * <p>
     * 处理拉取消息请求
     *
     * @param channel            通连接道
     * @param request            请求
     * @param brokerAllowSuspend broker是否支持挂起请求
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {
        //起始时间
        final long beginTimeMills = this.brokerController.getMessageStore().now();
        //创建响应命令对象
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        //创建响应头
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        //解析请求头
        final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        //设置请求id，通过id可以获取请求结果
        response.setOpaque(request.getOpaque());

        log.debug("receive PullMessage request command, {}", request);
        //当前broker是否可读，不可读则直接返回
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pulling message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        //获取当前consumerGroup对应的订阅信息
        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }
        //判断是否可消费，不可消费则直接返回
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }
        //是否支持请求挂起
        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        //是否提交消费进度
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        //是否存在子订阅，即TAG或者SQL92的设置，用于过滤消息
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());
        //计算broker最长的挂起时间，默认15s，该参数是消费者传递的
        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;
        //获取topic配置
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }
        //topic是否可读，不可读则直接返回
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }
        //校验请求中的队列id，如果小于0或者大于等于topic配置中的读队列数量，那么直接返回
        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]", requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }
        /*
         * 1 构建过滤信息
         * 真正的过滤消息操作还在后面，而且broker和consumer都会进行过滤
         */
        SubscriptionData subscriptionData = null;
        ConsumerFilterData consumerFilterData = null;
        //如果有子订阅标记，那么每次拉取消息都会重新构建subscriptionData和consumerFilterData，而不是使用缓存的信息，一般都是false
        //因为hasSubscriptionFlag为true需要consumer端将postSubscriptionWhenPull=true，并且订阅不是classFilter模式同时满足
        if (hasSubscriptionFlag) {
            try {
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType());
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getSubscription(), requestHeader.getExpressionType(), requestHeader.getSubVersion());
                    assert consumerFilterData != null;
                }
            } catch (Exception e) {
                log.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(), requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            //获取消费者组信息
            ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }
            //如果不支持广播消费但是消费者消费模式是广播消费，则直接返回
            if (!subscriptionGroupConfig.isConsumeBroadcastEnable() && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
                return response;
            }
            //获取broker缓存的此consumerGroupInfo中关于此topic的订阅关系
            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                log.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }
            //比较订阅关系版本
            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(), subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
            //如果订阅关系表达式不是TAG类型，那么构建consumerFilterData
            if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                consumerFilterData = this.brokerController.getConsumerFilterManager().get(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                if (consumerFilterData == null) {
                    response.setCode(ResponseCode.FILTER_DATA_NOT_EXIST);
                    response.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
                    return response;
                }
                if (consumerFilterData.getClientVersion() < requestHeader.getSubVersion()) {
                    log.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}", requestHeader.getConsumerGroup(), requestHeader.getTopic(), consumerFilterData.getClientVersion(), requestHeader.getSubVersion());
                    response.setCode(ResponseCode.FILTER_DATA_NOT_LATEST);
                    response.setRemark("the consumer's consumer filter data not latest");
                    return response;
                }
            }
        }
        //如果订阅关系表达式不是TAG类型，并且enablePropertyFilter没有开启支持SQL92，那么抛出异常
        //也就是说，如果消费者使用SQL92模式订阅，那么需要现在broker端设置enablePropertyFilter=true
        if (!ExpressionType.isTagType(subscriptionData.getExpressionType()) && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
            return response;
        }

        MessageFilter messageFilter;
        //重试topic是否支持filter过滤，默认false，即重试topic是不支持过滤额
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        } else {
            //创建普通的ExpressionMessageFilter，内部保存了消费者启动时通过心跳上报的订阅关系
            //一般基于tag订阅的情况下，consumerFilterData是null，通过subscriptionData进行过滤
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        }

        /*
         * 2 通过DefaultMessageStore#getMessage方法批量拉取消息，并且进行过滤操作
         */
        final GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);
        /*
         * 3 对于拉取结果GetMessageResult进行处理
         */
        if (getMessageResult != null) {
            //设置拉去状态枚举名字
            response.setRemark(getMessageResult.getStatus().name());
            //设置下次拉取的consumeQueue的起始逻辑偏移量
            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
            //设置consumeQueue的最小、最大的逻辑偏移量maxOffset和minOffset
            responseHeader.setMinOffset(getMessageResult.getMinOffset());
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset());
            /*
             * 3.1 判断并设置下次拉取消息的建议broker是MATER还是SLAVE
             */
            //是否建议从SLAVE拉取消息
            if (getMessageResult.isSuggestPullingFromSlave()) {
                //设置建议的brokerId为从服务器的id 1
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
            } else {
                //否则，设置建议的brokerId为主服务器的id 0
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }
            //判断broker角色
            switch (this.brokerController.getMessageStoreConfig().getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    break;
                case SLAVE:
                    //如果是SLAVE，并且从服务器不可读
                    if (!this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
                        //设置响应码为PULL_RETRY_IMMEDIATELY，consumer收到响应后会立即从MASTER重试拉取
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        //设置建议的brokerId为主服务器的id 0
                        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    }
                    break;
            }
            //如果从服务器可读
            if (this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
                // 如果消费太慢了，那么下次重定向到另一台broker，id通过subscriptionGroupConfig的whichBrokerWhenConsumeSlowly指定，默认1，即SLAVE
                if (getMessageResult.isSuggestPullingFromSlave()) {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                }
                // consume ok
                else {
                    //id通过subscriptionGroupConfig的brokerId指定，默认0，即MASTER
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                }
            } else {
                //如果从服务器不可读，设置建议的brokerId为主服务器的id 0
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }
            /*
             * 3.2 判断拉取消息状态码，并设置对应的响应码
             */
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    //找到了消息
                    response.setCode(ResponseCode.SUCCESS);
                    break;
                case MESSAGE_WAS_REMOVING:
                    //commitLog中没有找到消息
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    //没找到consumeQueue，或者consumeQueue没有消息
                    if (0 != requestHeader.getQueueOffset()) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);

                        // XXX: warn and notify me
                        log.info("the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}", requestHeader.getQueueOffset(), getMessageResult.getNextBeginOffset(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup());
                    } else {
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    //没匹配到消息
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}", requestHeader.getQueueOffset(), getMessageResult.getMaxOffset(), channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    log.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}", requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(), getMessageResult.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }
            /*
             * 3.3 判断如果有消费钩子，那么执行consumeMessageBefore方法
             */
            if (this.hasConsumeMessageHook()) {
                //构建上下文
                ConsumeMessageContext context = new ConsumeMessageContext();
                context.setConsumerGroup(requestHeader.getConsumerGroup());
                context.setTopic(requestHeader.getTopic());
                context.setQueueId(requestHeader.getQueueId());

                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);

                switch (response.getCode()) {
                    case ResponseCode.SUCCESS:
                        int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                        int incValue = getMessageResult.getMsgCount4Commercial() * commercialBaseCount;

                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
                        context.setCommercialRcvTimes(incValue);
                        context.setCommercialRcvSize(getMessageResult.getBufferTotalSize());
                        context.setCommercialOwner(owner);

                        break;
                    case ResponseCode.PULL_NOT_FOUND:
                        if (!brokerAllowSuspend) {

                            context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                            context.setCommercialRcvTimes(1);
                            context.setCommercialOwner(owner);

                        }
                        break;
                    case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    case ResponseCode.PULL_OFFSET_MOVED:
                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);
                        break;
                    default:
                        assert false;
                        break;
                }
                /*
                 * 执行前置钩子方法
                 */
                this.executeConsumeMessageHookBefore(context);
            }
            /*
             * 3.4 判断响应码，然后直接返回数据或者进行短轮询或者长轮询
             */
            switch (response.getCode()) {
                //如果拉取消息成功
                case ResponseCode.SUCCESS:
                    //更新一些统计信息
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), getMessageResult.getMessageCount());

                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(), getMessageResult.getBufferTotalSize());

                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                    //是否读取消息到堆内存中，默认true
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        //从buffer中读取出消息转换为字节数组
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        //设置到body中
                        response.setBody(r);
                    } else {
                        try {
                            //基于netty直接读取buffer传输
                            //真正的零拷贝，底层借助 Java NIO 的 FileChannel.transfer
                            FileRegion fileRegion = new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    getMessageResult.release();
                                    if (!future.isSuccess()) {
                                        log.error("transfer many message by pagecache failed, {}", channel.remoteAddress(), future.cause());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("transfer many message by pagecache exception", e);
                            getMessageResult.release();
                        }

                        response = null;
                    }
                    break;
                //没有读取到消息
                case ResponseCode.PULL_NOT_FOUND:
                    //如果broker允许挂起请求并且客户端支持请求挂起，则broker挂起该请求一段时间，中间如果有消息到达则会唤醒请求拉取消息并返回
                    if (brokerAllowSuspend && hasSuspendFlag) {
                        //broker最长的挂起时间，默认15s，该参数是消费者传递的
                        long pollingTimeMills = suspendTimeoutMillisLong;
                        //如果broker不支持长轮询，默认都是支持的
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            //那么使用短轮询，即最长的挂起时间设置为1s
                            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                        }

                        String topic = requestHeader.getTopic();
                        long offset = requestHeader.getQueueOffset();
                        int queueId = requestHeader.getQueueId();
                        //创建新的拉取请求
                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
                        //通过pullRequestHoldService#suspendPullRequest方法提交PullRequest，该请求将会被挂起并异步处理
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
                        response = null;
                        break;
                    }

                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                //读取的offset不正确，太大或者太小
                case ResponseCode.PULL_OFFSET_MOVED:
                    //如果broker不是SLAVE，或者是SLAVE，但是允许offset校验
                    if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
                        //发布offset移除事件
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(requestHeader.getTopic());
                        mq.setQueueId(requestHeader.getQueueId());
                        mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                        OffsetMovedEvent event = new OffsetMovedEvent();
                        event.setConsumerGroup(requestHeader.getConsumerGroup());
                        event.setMessageQueue(mq);
                        event.setOffsetRequest(requestHeader.getQueueOffset());
                        event.setOffsetNew(getMessageResult.getNextBeginOffset());
                        this.generateOffsetMovedEvent(event);
                        log.warn("PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}", requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(), responseHeader.getSuggestWhichBrokerId());
                    } else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}", requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset(), responseHeader.getSuggestWhichBrokerId());
                    }

                    break;
                default:
                    assert false;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }
        /*
         * 4 拉取消息完毕之后，无论是否拉取到消息，只要broker支持挂起请求，并且consumer支持提交消费进度，并且当前broker不是SLAVE角色，都会上报该消费者上一次的消费点位
         * 另外消费者客户端也会定时没5s上报一次消费点
         */
        //要求brokerAllowSuspend为true，新的拉取请求为true，但是已被suspend的请求将会是false
        boolean storeOffsetEnable = brokerAllowSuspend;
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
        storeOffsetEnable = storeOffsetEnable && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        //如果支持持久化偏移量
        if (storeOffsetEnable) {
            //上报偏移量
            this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannelRemoteAddr(channel), requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }
        return response;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic, final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION);
//                bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
//                IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos = 4 // 1 TOTALSIZE
                        + 4 // 2 MAGICCODE
                        + 4 // 3 BODYCRC
                        + 4 // 4 QUEUEID
                        + 4 // 5 FLAG
                        + 8 // 6 QUEUEOFFSET
                        + 8 // 7 PHYSICALOFFSET
                        + 4 // 8 SYSFLAG
                        + 8 // 9 BORNTIMESTAMP
                        + bornhostLength; // 10 BORNHOST
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    private void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }

    /**
     * PullMessageProcessor的方法
     * <p>
     * 唤醒后之后拉取操作
     *
     * @param channel
     * @param request
     * @throws RemotingCommandException
     */
    public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request) throws RemotingCommandException {
        //构建线程任务
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    /*
                     * 再一次调用processRequest方法
                     * 这里的brokerAllowSuspend参数为false，也就是说，本次请求如果没有拉取到消息，那么不会再挂起
                     */
                    final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false);

                    if (response != null) {
                        //获取请求id，通过id可以获取请求结果，标记响应状态
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {
                            //将响应写回给客户端
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        log.error("processRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
                                        log.error(request.toString());
                                        log.error(response.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("processRequestWrapper process request over, but response failed", e);
                            log.error(request.toString());
                            log.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    log.error("excuteRequestWhenWakeup run", e1);
                }
            }
        };
        //通过pullMessageExecutor线程池异步的处理拉取消息的请求
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }
}
