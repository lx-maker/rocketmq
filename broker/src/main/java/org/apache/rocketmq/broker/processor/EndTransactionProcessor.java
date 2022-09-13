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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * EndTransaction processor: process commit and rollback message
 */
public class EndTransactionProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * EndTransactionProcessor的方法
     * <p>
     * 处理END_TRANSACTION请求，处理事务消息的提交或者回滚
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
            RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //解析请求头
        final EndTransactionRequestHeader requestHeader =
                (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.debug("Transaction request:{}", requestHeader);
        //1 如果是SLAVE broker，直接返回，只有MASTER broker能够处理事务消息
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }
        /*
         * 2 判断本地事务执行状态，如果是TRANSACTION_NOT_TYPE，那么表示本地事务没有结果，可能是还在等待事务结束，broker将不会不进行任何处理，直接返回
         */
        //如果当前请求来自于事务回查消息
        if (requestHeader.getFromTransactionCheck()) {
            switch (requestHeader.getCommitOrRollback()) {
                //事务回查没有结果，可能是还在等待事务结束，broker不进行任何处理，直接返回
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    return null;
                }
                //事务回查结果为提交，将会提交该消息
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());

                    break;
                }
                //事务回查结果为回滚，将会回滚该消息
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }
        //如果当前请求来自于二阶段endTransaction结束事务消息
        else {
            switch (requestHeader.getCommitOrRollback()) {
                //本地事务状态没有结果，可能是还在等待事务结束，broker不进行任何处理，直接返回
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    return null;
                }
                //本地事务结果为提交，将会提交该消息
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }
                //本地事务结果为回滚，将会回滚该消息
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }

        OperationResult result = new OperationResult();
        /*
         * 3 提交事务
         */
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            /*
             * 提交half消息，但实际上仅仅是根据commitLogOffset查询half消息
             */
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
            //查询到half消息
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                //检查half消息
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                //检查通过
                if (res.getCode() == ResponseCode.SUCCESS) {
                    /*
                     * 还原原始的消息
                     */
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                    //设置系统标记。重置事物标记
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    /*
                     * 内部调用asyncPutMessage方法发送消息到原始的topic，随后consumer可以消费到该消息
                     */
                    RemotingCommand sendResult = sendFinalMessage(msgInner);
                    //如果发送成功
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {
                        /*
                         * 删除half消息
                         */
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }
        }
        /*
         * 4 回滚事务
         */
        else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
            /*
             * 回滚half消息，但实际上仅仅是根据commitLogOffset查询half消息
             */
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            //查询到half消息
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                //检查half消息
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                //检查通过
                if (res.getCode() == ResponseCode.SUCCESS) {
                    /*
                     * 删除half消息
                     */
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * TransactionalMessageServiceImpl
     * <p>
     * 检查half消息
     *
     * @param msgExt        half消息
     * @param requestHeader 请求头
     */
    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            //获取生产者组
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            //如果消息的生产者组和请求头中的producerGroup不一致，则检查失败
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }
            //如果消息的ConsumeQueue offset和请求头中的offset不一致，则检查失败
            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }
            //如果消息的CommitLog offset和请求头中的CommitLogOffset不一致，则检查失败
            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    /**
     * EndTransactionProcessor的方法
     * <p>
     * 还原原始消息
     *
     * @param msgExt 事物消息
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        //设置topic为原始topic
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        //设置queueId为原始id
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        //设置消息事物id，即客户端生成的uniqId
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
                (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                        : TopicFilterType.SINGLE_TAG;
        //生成tagsCode
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        //清除没用的属性
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    /**
     * EndTransactionProcessor的方法
     * <p>
     * 内部调用asyncPutMessage方法发送消息到原始的topic，随后consumer可以消费到该消息
     */
    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        /*
         * 同步的将最终的消息发送到原始的topic，随后consumer可以消费到该消息
         * 内部实现为：调用asyncPutMessage方法异步发送消息，调用putMessageResultFuture#get方法同步等待结果
         */
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        //处理响应结果
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
