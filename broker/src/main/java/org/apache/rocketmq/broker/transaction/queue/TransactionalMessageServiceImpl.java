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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 以异步方式处理事务准备消息
     *
     * @param messageInner 事务准备消息，也就是half消息
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        //异步的存放半消息
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 通过检查当前回查次数是否大于等于最大回查次数来判断是否丢弃消息
     *
     * @param msgExt              half消息
     * @param transactionCheckMax 族弟啊检查次数，默认15
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        //从PROPERTY_TRANSACTION_CHECK_TIMES属性获取回查次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            //如果回查次数大于等于最大值，那么需要丢弃
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                //否则，回查次数自增1
                checkTime++;
            }
        }
        //回查次数设置到属性中
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 通过检查消息时间判断是否需要跳过该消息
     *
     * @param msgExt half消息
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        //当前时间戳减去消息发送时间戳
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        //如果中间间隔的时间大于fileReservedTime，则跳过该消息，fileReservedTime为消息日志文件保留的时间默认72h，即3天
        if (valueOfCurrentMinusBorn
                > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
                * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                    msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                    putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                    putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                    "Send check message, the offset={} restored in queueOffset={} "
                            + "commitLogOffset={} "
                            + "newMsgId={} realMsgId={} topic={}",
                    offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                    msgExt.getTopic());
            return true;
        } else {
            log.error(
                    "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                            + "msgId: {}",
                    msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 执行事务检查和回查
     *
     * @param transactionTimeout  事务超时时间，默认6s，即超过6s还没有被commit或者rollback的事物消息将会进行回查
     * @param transactionCheckMax 消息被检查的最大次数，默认15，如果超过该值，该消息将被丢弃
     * @param listener            当需要发起回查或者丢弃消息时，会调用相应的方法
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
                      AbstractTransactionalMessageCheckListener listener) {
        try {
            /*
             * 1 获取事物half消息的topic RMQ_SYS_TRANS_HALF_TOPIC下的所有mq，默认就一个
             */
            //事物half消息的topic
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            //获取该topic下的所有mq，默认就一个
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            /*
             * 2 遍历事物half消息的mq，依次进行检测
             */
            for (MessageQueue messageQueue : msgQueues) {
                //起始时间
                long startTime = System.currentTimeMillis();
                /*
                 * 2.1 获取对应的Op消息队列，half消息队列和Op消息队列是一一对应的关系
                 */
                MessageQueue opQueue = getOpQueue(messageQueue);
                /*
                 * 2.2 获取消费偏移量
                 */
                //获取内部消费者组CID_SYS_RMQ_TRANS对于该half mq的消费偏移量
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                //获取内部消费者组CID_SYS_RMQ_TRANS对于该Op mq的消费偏移量
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                            halfOffset, opOffset);
                    continue;
                }
                //halfOffset < 最新消费的halfOffset的消息，已处理完成的消息，value：opOffset
                List<Long> doneOpOffset = new ArrayList<>();
                //halfOffset >= 最新消费的halfOffset，需要移除的消息，key：halfOffset，value：opOffset
                HashMap<Long, Long> removeMap = new HashMap<>();
                /*
                 * 2.3 根据最新已处理的op消息队列消费偏移量和half消息队列消费偏移量，拉取op消息，填充removeMap和doneOpOffset，找出已处理的half消息，避免重复发送事物状态回查请求
                 */
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                //没拉取到
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                            messageQueue, halfOffset, opOffset);
                    continue;
                }
                /*
                 * 2.4 从最新消费的halfOffset开始循环进行检测
                 */
                // single thread
                //获取空消息的次数
                int getMessageNullCount = 1;
                //处理的最新的half消息偏移量
                long newOffset = halfOffset;
                //从最新消费的halfOffset开始遍历
                long i = halfOffset;
                while (true) {
                    /*
                     * 2.4.1 每一轮消息回查最多进行60s，超时就退出，检测下一个队列
                     */
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    /*
                     * 2.4.2 如果removeMap中已包含该offset，从removeMap移除并且加入到doneOpOffset，那么表示已经确定了的事物消息，无需回查
                     */
                    //如果removeMap中已包含该offset，那么表示已经确定了的事物消息，无需回查
                    if (removeMap.containsKey(i)) {
                        log.debug("Half offset {} has been committed/rolled back", i);
                        //从removeMap移除并且加入到doneOpOffset
                        Long removedOpOffset = removeMap.remove(i);
                        doneOpOffset.add(removedOpOffset);
                    }
                    /*
                     * 2.4.3 否则，表示可能需要回查
                     */
                    else {
                        /*
                         * 2.4.4 根据offset查询该half 事物消息
                         */
                        //根据offset查询该half 事物消息
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        /*
                         * 2.4.5 如果没找到消息
                         */
                        if (msgExt == null) {
                            //判断是否可以重试，最多重试一次，如果超过次数则结束该消息队列的回查
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            //没有消息
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                        messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                        i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                //重置
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }
                        /*
                         * 2.4.6 判断是否需要丢弃、跳过该消息
                         */
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            //通过listener丢弃该half消息，即将消息存入TRANS_CHECK_MAX_TIME_TOPIC这个内部topic中
                            listener.resolveDiscardMsg(msgExt);
                            //增加offset
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        /*
                         * 2.4.7 判断事务是否到达超时时间，超时后才会检测
                         */
                        //消息存储时间大于本次回查开始时间，那么本消息队列回查结束
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                    new Date(msgExt.getStoreTimestamp()));
                            break;
                        }
                        //当前时间戳 减去 消息发送时间戳，得到消息已经存储的时间戳
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        //立即检测事务消息的时间，初始化为 事务消息的超时时间，默认为 6s，这个时间是broker中设置的
                        long checkImmunityTime = transactionTimeout;
                        //从half消息的PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性中获取consumer客户端设置的事务消息检测时间
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        //如果设置了该属性，一般是没人设置的
                        if (null != checkImmunityTimeStr) {
                            //如果consumer设置了事务超时时间，那么就使用自己设置的时间，否则使用broker端的默认超时时间6s
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            //如果消息存储的时间小于事务超时时间，那么说明还没到事务回查的时候
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                //检查half队列偏移量，返回true则跳过该消息
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    //跳过该消息
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            //如果没有设置PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性，并且消息存储的时间小于事务超时时间
                            //那么说明还没到事务回查的时候，当前mq的回查结束
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                        checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        /*
                         * 2.4.6 判断是否需要检测
                         * 如果拉取的op消息为null并且当前消息存储的时间大于事务超时时间
                         * 或者拉取的op消息不为null并且最后一个op消息的发送存储时减去起始时间的结果大于事务超时时间
                         * 或者当前时间小于当前消息发送时间戳
                         * 这3种情况都会检测
                         */
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                                || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                || (valueOfCurrentMinusBorn <= -1);
                        /*
                         * 2.4.7 执行回查
                         */
                        if (isNeedCheck) {
                            //首先将该消息再次存入half队列
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            //然后通过listener向consumer客户端发起一个单向消息回查请求
                            listener.resolveHalfMsg(msgExt);
                        }
                        /*
                         * 2.4.8 如果不需要执行回查，那么从已拉取的op消息的下一个offset开始，再次执行fillOpRemoveMap，拉取下一轮的op消息，继续下一个循环检测
                         */
                        else {
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.debug("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                    messageQueue, pullResult);
                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                //更新half消息队列偏移量
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                //更新op消息队列偏移量
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 读取op消息，解析op消息，填充removeMap
     *
     * @param removeMap      要删除的half消息，key：halfOffset，value：opOffset
     * @param opQueue        Op消息队列
     * @param pullOffsetOfOp Op消息队列的开始偏移量
     * @param miniOffset     half消息队列的当前最小偏移量。
     * @param doneOpOffset   已处理的Op消息 ，value：opOffset
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
                                       MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        //通过CID_SYS_RMQ_TRANS消费者组拉取32条最新Op消息
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        //请求offset不合法，过大或者过小
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            return pullResult;
        }
        //获取拉取到的Op消息
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        //遍历Op消息
        for (MessageExt opMessageExt : opMsg) {
            //解析Op消息的消息体，结果就是对应的half消息在half 消息队列的相对偏移量
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            //是否有d的tag标记
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                //如果有标记，并且小于最新的half消息消费偏移量
                if (queueOffset < miniOffset) {
                    //加入到doneOpOffset集合，表示已处理的half消息
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    //加入到removeMap集合，表示当前half消息需要移除 key：halfOffset，value：opOffset。
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * 检查half消息队列的偏移量，返回true则跳过该消息
     *
     * @param removeMap    需要移除的消息
     * @param doneOpOffset 已处理完成的消息
     * @param msgExt       half消息
     * @return 返回true则跳过该消息
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
                                            MessageExt msgExt) {
        //从该消息获取PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性，即此前该消息在half队列的offset，也就是第一次存放该消息的offset
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        //如果没有该属性，说明该消息第一次遇见
        if (null == prepareQueueOffsetStr) {
            //将该消息重新存入half队列，等待下一次的回查，存放成功则返回true
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            //获取该属性值，也就是第一次存放该消息的offset
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                //如果removeMap包含该offset，那么移除并加入doneOpOffset，
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    //此时表示该消息状态已确定，不需要回查
                    return true;
                } else {
                    //将该消息重新存入half队列，等待下一次的回查，存放成功则返回true
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }


    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        //重建一个MessageExtBrokerInner，将最开始的消息的offset存入PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性中
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        //消息存入half队列
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq     Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums   Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq     Target Message Queue
     * @param offset Offset in the message queue
     * @param nums   Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 获取对应的Op消息队列，half消息队列和Op消息队列是一一对应的关系
     *
     * @param messageQueue half消息队列
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        //从opQueueMap缓存中尝试直接获取
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            //如果没获取到，则创建一个Op消息队列，topic为RMQ_SYS_TRANS_OP_HALF_TOPIC，brokerName和queueId和对应的half消息队列的属性一致。
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                    messageQueue.getQueueId());
            //存入缓存
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 根据commitLogOffset查询half消息
     */
    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        //根据commitLogOffset查询half消息
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        //找到了消息就设置SUCCESS
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 当提交或回滚消息时，删除half消息，Op消息的逻辑
     *
     * @param msgExt half消息
     */
    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        //写入事务Op消息，opType为“d”
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 提交half消息，但实际上仅仅是根据commitLogOffset查询half消息
     */
    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        //根据commitLogOffset查询half消息
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    /**
     * TransactionalMessageServiceImpl的方法
     * <p>
     * 回滚half消息，但实际上仅仅是根据commitLogOffset查询half消息
     */
    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        //根据commitLogOffset查询half消息
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
