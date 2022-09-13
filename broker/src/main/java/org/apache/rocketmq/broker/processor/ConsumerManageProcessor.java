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

import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public ConsumerManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * ConsumerManageProcessor的方法
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                //返回指定group的所有客户端id集合
                return this.getConsumerListByGroup(ctx, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                //更新消费偏移量
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                //查询消费偏移量
                return this.queryConsumerOffset(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * ConsumerManageProcessor的方法
     * <p>
     * 返回指定group的所有客户端id集合
     */
    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        //创建响应命令对象
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        //解析请求头
        final GetConsumerListByGroupRequestHeader requestHeader =
                (GetConsumerListByGroupRequestHeader) request
                        .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
        //从broker的consumerTable中获取指定group的消费者组信息
        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(
                        requestHeader.getConsumerGroup());

        if (consumerGroupInfo != null) {
            //获取所有客户端id集合
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                response.setBody(body.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            } else {
                log.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    /**
     * ConsumerManageProcessor的方法
     * <p>
     * 更新消费偏移量
     */
    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        //构建响应命令对象
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        //解析请求头
        final UpdateConsumerOffsetRequestHeader requestHeader =
                (UpdateConsumerOffsetRequestHeader) request
                        .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        //调用ConsumerOffsetManager#commitOffset方法
        this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * ConsumerManageProcessor的方法
     * <p>
     * 查询消费偏移量
     */
    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        //响应对象
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
                (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
        final QueryConsumerOffsetRequestHeader requestHeader =
                (QueryConsumerOffsetRequestHeader) request
                        .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);
        //根据 ConsumerGroup、Topic、QueueId 查询offset，实际上是从broker端的offsetTable这个map集合缓存属性中获取
        //在broker启动时就从broker的{user.home}/store/config/consumerOffset.json中加载
        long offset =
                this.brokerController.getConsumerOffsetManager().queryOffset(
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

        if (offset >= 0) {
            //大于等于0，则存入响应结果返回
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        }
        //小于0表示在offsetTable缓存中没找到
        else {
            //根据topic和queueId获取消费队列ConsumeQueue的最小的逻辑偏移量offset
            long minOffset =
                    this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(),
                            requestHeader.getQueueId());
            //如果消费队列最新偏移量小于等于0，并且该消费队列的0偏移量数据还在内存中，表示为新消息队列并且消息未清理过，并且数据量不是很大
            if (minOffset <= 0
                    && !this.brokerController.getMessageStore().checkInDiskByConsumeOffset(
                    requestHeader.getTopic(), requestHeader.getQueueId(), 0)) {
                //返回0，从0开始读取消费
                responseHeader.setOffset(0L);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            } else {
                //-1
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return response;
    }
}
