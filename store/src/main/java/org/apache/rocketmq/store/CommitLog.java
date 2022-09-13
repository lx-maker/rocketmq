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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    protected final static int BLANK_MAGIC_CODE = -875286124;
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMessageStore defaultMessageStore;
    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    private final FlushCommitLogService commitLogService;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    protected Map<String/* topic-queueid */, Long/* offset */> lmqTopicQueueTable = new ConcurrentHashMap<>(1024);
    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;

    protected final PutMessageLock putMessageLock;

    private volatile Set<String> fullStorePaths = Collections.emptySet();

    private final MultiDispatch multiDispatch;
    private final FlushDiskWatcher flushDiskWatcher;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        String storePath = defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog();
        if (storePath.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(defaultMessageStore.getMessageStoreConfig(),
                    defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                    defaultMessageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            this.mappedFileQueue = new MappedFileQueue(storePath,
                    defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                    defaultMessageStore.getAllocateMappedFileService());
        }

        this.defaultMessageStore = defaultMessageStore;

        //CommitLog的构造器

        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            //如果是同步刷盘，则初始化GroupCommitService服务
            this.flushCommitLogService = new GroupCommitService();
        } else {
            //如果是异步刷盘，则初始化GroupCommitService服务
            this.flushCommitLogService = new FlushRealTimeService();
        }
        //异步转存数据服务：将堆外内存的数据提交到fileChannel
        this.commitLogService = new CommitRealTimeService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

        this.multiDispatch = new MultiDispatch(defaultMessageStore, this);

        flushDiskWatcher = new FlushDiskWatcher();
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    /**
     * CommitLog的方法
     */
    public boolean load() {
        //调用mappedFileQueue的load方法
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushCommitLogService.start();

        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();


        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();

        flushDiskWatcher.shutdown(true);
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * CommitLog的方法
     * <p>
     * 获取CommitLog的数据
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        //获取CommitLog文件大小，默认1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        //根据指定的offset从mappedFileQueue中对应的CommitLog文件的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            //通过指定物理偏移量，除以文件大小，得到指定的相对偏移量
            int pos = (int) (offset % mappedFileSize);
            //从指定相对偏移量开始截取一段ByteBuffer，这段内存存储着将要重放的消息。
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     * 当正常退出、数据恢复时，所有内存数据都已刷到磁盘
     *
     * @param maxPhyOffsetOfConsumeQueue consumequeue文件中记录的最大有效commitlog文件偏移量
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        //是否需要校验文件CRC32，默认true
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        //获取commitlog文件集合
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        //如果存在commitlog文件
        if (!mappedFiles.isEmpty()) {
            //从倒数第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            //不足3个文件，则直接从第一个文件开始恢复。
            if (index < 0)
                index = 0;
            //获取文件对应的映射对象
            MappedFile mappedFile = mappedFiles.get(index);
            //文件映射对应的DirectByteBuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //获取文件映射的初始物理偏移量，其实和文件名相同
            long processOffset = mappedFile.getFileFromOffset();
            //当前commitlog映射文件的有效offset
            long mappedFileOffset = 0;
            while (true) {
                //生成DispatchRequest，验证本条消息是否合法
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                //获取消息大小
                int size = dispatchRequest.getMsgSize();
                //如果消息是正常的
                if (dispatchRequest.isSuccess() && size > 0) {
                    //更新mappedFileOffset的值加上本条消息长度
                    mappedFileOffset += size;
                }
                //如果消息征正常但是size为0，表示到了文件的末尾，则尝试跳到下一个commitlog文件进行检测
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    //如果最后一个文件查找完毕，结束循环
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        //如果最后一个文件没有查找完毕，那么跳转到下一个文件
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                //如果当前消息异常，那么不继续校验
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }
            //commitlog文件的最大有效区域的偏移量
            processOffset += mappedFileOffset;
            //设置当前commitlog下面的所有的commitlog文件的最新数据
            //设置刷盘最新位置，提交的最新位置
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            /*
             * 删除文件最大有效数据偏移量processOffset之后的所有数据
             */
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            //如果consumequeue文件记录的最大有效commitlog文件偏移量 大于等于 commitlog文件本身记录的最大有效区域的偏移量
            //那么以commitlog文件的有效数据为准，再次清除consumequeue文件中的脏数据
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            //如果不存在commitlog文件
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            //那么重置刷盘最新位置，提交的最新位置，并且清除所有的consumequeue索引文件
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * CommitLog的方法
     *
     * @param byteBuffer 一段内存
     * @param checkCRC   是否校验CRC
     * @param readBody   是否读取消息体
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            //消息条目总长度
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            //消息的magicCode属性，魔数，用来判断消息是正常消息还是空消息
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    //读取到文件末尾
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];
            //消息体CRC校验码
            int bodyCRC = byteBuffer.getInt();
            //消息消费队列id
            int queueId = byteBuffer.getInt();
            //消息flag
            int flag = byteBuffer.getInt();
            //消息在消息消费队列的偏移量
            long queueOffset = byteBuffer.getLong();
            //消息在commitlog中的偏移量
            long physicOffset = byteBuffer.getLong();
            //消息系统flag，例如是否压缩、是否是事务消息
            int sysFlag = byteBuffer.getInt();
            //消息生产者调用消息发送API的时间戳
            long bornTimeStamp = byteBuffer.getLong();
            //消息发送者的IP和端口号
            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            //消息存储时间
            long storeTimestamp = byteBuffer.getLong();
            //broker的IP和端口号
            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            //消息重试次数
            int reconsumeTimes = byteBuffer.getInt();
            //事务消息物理偏移量
            long preparedTransactionOffset = byteBuffer.getLong();
            //消息体长度
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                //读取消息体
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    //不需要读取消息体，那么跳过这段内存
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            //Topic名称内容大小
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            //topic的值
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;
            //消息属性大小
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                //消息属性
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                //客户端生成的uniqId，也被称为msgId，从逻辑上代表客户端生成的唯一一条消息
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                //tag
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                //普通消息的tagsCode被设置为tag的hashCode
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                /*
                 * 延迟消息处理
                 * 对于延迟消息，tagsCode被替换为延迟消息的发送时间，主要用于后续判断消息是否到期
                 */
                {
                    //消息属性中获取延迟级别DELAY字段，如果是延迟消息则生产者会在构建消息的时候设置进去
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    //如果topic是SCHEDULE_TOPIC_XXXX，即延迟消息的topic
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            //tagsCode被替换为延迟消息的发送时间，即真正投递时间
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                    storeTimestamp);
                        }
                    }
                }
            }
            //读取的当前消息的大小
            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            //不相等则记录BUG
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }
            //根据读取的消息属性内容，构建为一个DispatchRequest对象并返回
            return new DispatchRequest(
                    topic,
                    queueId,
                    physicOffset,
                    totalSize,
                    tagsCode,
                    storeTimestamp,
                    queueOffset,
                    keys,
                    uniqKey,
                    sysFlag,
                    preparedTransactionOffset,
                    propertiesMap
            );
        } catch (Exception e) {
        }
        //读取异常
        return new DispatchRequest(-1, false /* success */);
    }

    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + bornhostLength //BORNHOST
                + 8 //STORETIMESTAMP
                + storehostAddressLength //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    /**
     * CommitLog的方法
     * <p>
     * broker异常退出时的commitlog文件恢复，按最小时间戳恢复
     *
     * @param maxPhyOffsetOfConsumeQueue consumequeue文件中记录的最大有效commitlog文件偏移量
     */
    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {

        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            //倒叙遍历所有的commitlog文件执行检查恢复
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                //首先校验当前commitlog文件是否是一个正确的文件
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }
            /*
             * 从第一个正确的commitlog文件开始遍历恢复
             */
            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //获取文件映射的初始物理偏移量，其实和文件名相同
            long processOffset = mappedFile.getFileFromOffset();
            //commitlog映射文件的有效offset
            long mappedFileOffset = 0;
            while (true) {
                //生成DispatchRequest，验证本条消息是否合法
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                //获取消息大小
                int size = dispatchRequest.getMsgSize();
                //如果消息是正常的
                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        //更新mappedFileOffset的值加上本条消息长度
                        mappedFileOffset += size;
                        //如果消息允许重复复制，默认为 false
                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            //如果消息物理偏移量小于CommitLog的提交指针
                            //则调用CommitLogDispatcher重新构建当前消息的indexfile索引和consumequeue索引
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            //调用CommitLogDispatcher重新构建当前消息的indexfile索引和consumequeue索引
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    //如果消息正常但是size为0，表示到了文件的末尾，则尝试跳到下一个commitlog文件进行检测
                    else if (size == 0) {
                        index++;
                        //如果最后一个文件查找完毕，结束循环
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            //如果最后一个文件没有查找完毕，那么跳转到下一个文件
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {
                    //如果当前消息异常，那么不继续校验
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }
            //commitlog文件的最大有效区域的偏移量
            processOffset += mappedFileOffset;
            //设置当前commitlog下面的所有的commitlog文件的最新数据
            //设置刷盘最新位置，提交的最新位置
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            /*
             * 删除文件最大有效数据偏移量processOffset之后的所有数据
             */
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            //如果consumequeue文件记录的最大有效commitlog文件偏移量 大于等于 commitlog文件本身记录的最大有效区域的偏移量
            //那么以commitlog文件的有效数据为准，再次清除consumequeue文件中的脏数据
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            //如果不存在commitlog文件
            //那么重置刷盘最新位置，提交的最新位置，并且清除所有的consumequeue索引文件
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    /**
     * CommitLog的方法
     *
     * @param mappedFile 需要判断的commitog文件
     */
    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        //获取文件开头的魔数
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        //如果文件的魔数与commitlog文件的正确的魔数不一致，则直接返回false，表示不是正确的commitlog文件
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }
        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        //如果消息存盘时间为0，则直接返回false，表示未存储任何消息
        if (0 == storeTimestamp) {
            return false;
        }
        //如果messageIndexEnable为true，并且使用安全的消息索引功能，即可靠模式，那么Index文件进行校验
        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            //如果StoreCheckpoint的最小刷盘时间戳大于等于当前文件的存储时间，那么返回true，表示当前文件至少有部分是可靠的
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            //如果文件最小的最新消息刷盘时间戳大于等于当前文件的存储时间，那么返回true，表示当前文件至少有部分是可靠的
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    private String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    /**
     * CommitLog的方法
     * <p>
     * 异步存储消息
     *
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        //设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        //设置消息正文CRC
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
//        int queueId msg.getQueueId();
        /*
         * 1 处理延迟消息的逻辑
         *
         * 替换topic和queueId，保存真实topic和queueId
         */
        //根据sysFlag获取事务状态，普通消息的sysFlag为0
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        //如果不是事务消息，或者commit提交事务消息
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            //获取延迟级别，判断是否是延迟消息
            if (msg.getDelayTimeLevel() > 0) {
                //如果延迟级别大于最大级别，则设置为最大级别
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }
                //获取延迟队列的topic，固定为 SCHEDULE_TOPIC_XXXX
                topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                //根据延迟等级获取对应的延迟队列id， id = level - 1
                int queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                //使用扩展属性REAL_TOPIC 记录真实topic
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                //使用扩展属性REAL_QID 记录真实queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
                //更改topic和queueId为延迟队列的topic和queueId
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }
        //发送消息的地址
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }
        //存储消息的地址
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }
        /*
         * 2 消息编码
         */
        //获取线程本地变量，其内部包含一个线程独立的encoder和keyBuilder对象
        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        //将消息内容编码，存储到encoder内部的encoderBuffer中，它是通过ByteBuffer.allocateDirect(size)得到的一个直接缓冲区
        //消息写入之后，会调用encoderBuffer.flip()方法，将Buffer从写模式切换到读模式，可以读取到数据
        PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
        if (encodeResult != null) {
            return CompletableFuture.completedFuture(encodeResult);
        }
        //编码后的encoderBuffer暂时存入msg的encodedBuff中
        msg.setEncodedBuff(putMessageThreadLocal.getEncoder().encoderBuffer);
        //存储消息上下文
        PutMessageContext putMessageContext = new PutMessageContext(generateKey(putMessageThreadLocal.getKeyBuilder(), msg));
        /*
         * 3 加锁并写入消息
         * 一个broker将所有的消息都追加到同一个逻辑CommitLog日志文件中，因此需要通过获取putMessageLock锁来控制并发。
         */
        //持有锁的时间
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        /*
         * 有两种锁，一种是ReentrantLock可重入锁，另一种spin则是CAS锁
         * 根据StoreConfig的useReentrantLockWhenPutMessage决定是否使用可重入锁，默认为true，使用可重入锁。
         */
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            /*
             * 从mappedFileQueue中的mappedFiles集合中获取最后一个MappedFile
             */
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            //加锁后的起始时间
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            //设置存储的时间戳为加锁后的起始时间，保证有序
            msg.setStoreTimestamp(beginLockTimestamp);
            /*
             * 如果最新mappedFile为null，或者mappedFile满了，那么会新建mappedFile并返回
             */
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }
            /*
             *  追加存储消息
             */
            result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    //文件剩余空间不足，那么初始化新的文件并尝试再次存储
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }
            //加锁的持续时间
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
        } finally {
            //重置开始时间，释放锁
            beginTimeInLock = 0;
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }
        //如果存在写满的MappedFile并且启用了文件内存预热，那么这里对MappedFile执行解锁
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        //存储数据的统计信息更新
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(1);
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());
        /*
         * 4 提交刷盘请求，将会根据刷盘策略进行刷盘
         */
        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, msg);
        /*
         * 5 提交副本请求，用于主从同步
         */
        CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, msg);
        //当结果出来之后进行处理，对于同步刷盘，此处会进行等待
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        PutMessageContext putMessageContext = new PutMessageContext(generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch));
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                default:
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
        } finally {
            beginTimeInLock = 0;
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        CompletableFuture<PutMessageStatus> flushOKFuture = submitFlushRequest(result, messageExtBatch);
        CompletableFuture<PutMessageStatus> replicaOKFuture = submitReplicaRequest(result, messageExtBatch);
        return flushOKFuture.thenCombine(replicaOKFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });

    }

    /**
     * CommitLog的方法
     * <p>
     * 提交刷盘请求
     */
    public CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result, MessageExt messageExt) {
        // Synchronization flush
        /*
         * 同步刷盘策略
         */
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            //获取同步刷盘服务GroupCommitService
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            //判断消息的配置是否需要等待存储完成后才返回
            if (messageExt.isWaitStoreMsgOK()) {
                //同步刷盘并且需要等待刷刷盘结果

                //构建同步刷盘请求 刷盘偏移量nextOffset = 当前写入偏移量 + 当前消息写入大小
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                        this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                //将请求加入到刷盘监视器内部的commitRequests中
                flushDiskWatcher.add(request);
                //将请求存入内部的requestsWrite，并且唤醒同步刷盘线程
                service.putRequest(request);
                //仅仅返回future，没有填充结果
                return request.future();
            } else {
                //同步刷盘但是不需要等待刷刷盘结果，那么唤醒同步刷盘线程，随后直接返回PUT_OK
                service.wakeup();
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }
        // Asynchronous flush
        /*
         * 异步刷盘策略
         */
        else {
            //是否启动了堆外缓存
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                //如果没有启动了堆外缓存，那么唤醒异步刷盘服务FlushRealTimeService
                flushCommitLogService.wakeup();
            } else {
                //如果启动了堆外缓存，那么唤醒异步转存服务CommitRealTimeService
                commitLogService.wakeup();
            }
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    public CompletableFuture<PutMessageStatus> submitReplicaRequest(AppendMessageResult result, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                if (service.isSlaveOK(result.getWroteBytes() + result.getWroteOffset())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                            this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    return request.future();
                } else {
                    return CompletableFuture.completedFuture(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * CommitLog的方法
     * <p>
     * 根据消息的物理偏移量和消息大小截取消息所属的一段内存
     *
     * @param offset 消息的物理偏移量
     * @param size 消息大小
     * @return 截取消息所属的一段内存
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        //获取CommitLog文件大小，默认1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        //根据offset找到其所属的CommitLog文件对应的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            //该mappedFile的起始偏移量
            int pos = (int) (offset % mappedFileSize);
            //从pos开始截取size大小的一段buffer内存
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
            this.lmqTopicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    public Map<String, Long> getLmqTopicQueueTable() {
        return this.lmqTopicQueueTable;
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 异步堆外缓存刷盘服务
     */
    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        /**
         * CommitRealTimeService
         * <p>
         * 执行异步堆外缓存刷盘服务
         */
        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            /*
             * 运行时逻辑
             * 如果服务没有停止，则在死循环中执行刷盘的操作
             */
            while (!this.isStopped()) {
                //获取刷盘间隔时间，默认200ms，可通过commitIntervalCommitLog配置
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                //获取刷盘的最少页数，默认4，即16k，可通过commitCommitLogLeastPages配置
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();
                //最长刷盘延迟间隔时间，默认200ms，可通过commitCommitLogThoroughInterval配置，即距离上一次刷盘超过200ms时，不管页数是否超过4，都会刷盘
                int commitDataThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                //如果当前时间距离上次刷盘时间大于等于200ms，那么必定刷盘
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    /*
                     * 调用commit方法提交数据，而不是直接flush
                     */
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    //如果已经提交了一些脏数据到fileChannel
                    if (!result) {
                        //更新最后提交的时间戳
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        //唤醒flushCommitLogService异步刷盘服务进行刷盘操作
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    //等待执行
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }
            /*
             * 停止时逻辑
             * 在正常情况下服务关闭时，一次性执行10次刷盘操作
             */
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 异步刷盘服务
     */
    class FlushRealTimeService extends FlushCommitLogService {
        //最后一次刷盘时间戳
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        /**
         * FlushRealTimeService的方法
         */
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            /*
             * 运行时逻辑
             * 如果服务没有停止，则在死循环中执行刷盘的操作
             */
            while (!this.isStopped()) {
                //是否是定时刷盘，默认是false，即不开启，可通过flushCommitLogTimed配置
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                //获取刷盘间隔时间，默认500ms，可通过flushIntervalCommitLog配置
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                //获取刷盘的最少页数，默认4，即16k，可通过flushCommitLogLeastPages配置
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();
                //最长刷盘延迟间隔时间，默认10s，可通过flushCommitLogThoroughInterval配置，即距离上一次刷盘超过10S时，不管页数是否超过4，都会刷盘
                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                //如果当前时间距离上次刷盘时间大于等于10s，那么必定刷盘
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    //更新刷盘时间戳为当前时间
                    this.lastFlushTimestamp = currentTimeMillis;
                    //最少刷盘页数为0，即不管页数是否超过4，都会刷盘
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    //判断是否是定时刷盘
                    if (flushCommitLogTimed) {
                        //如果定时刷盘，那么当前线程睡眠指定的间隔时间
                        Thread.sleep(interval);
                    } else {
                        //如果不是定时刷盘，那么调用waitForRunning方法，线程最多阻塞指定的间隔时间
                        //可以被中途的wakeup方法唤醒进而直接尝试进行刷盘
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }
                    /*
                     * 开始刷盘
                     */
                    long begin = System.currentTimeMillis();
                    /*
                     * 刷盘指定的页数
                     */
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    //获取存储时间戳
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    //修改StoreCheckpoint中的physicMsgTimestamp：最新commitlog文件的刷盘时间戳，单位毫秒
                    //这里用于重启数据恢复
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    //刷盘消耗时间
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            /*
             * 停止时逻辑
             * 在正常情况下服务关闭时，一次性执行刷盘操作
             */
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public long getDeadLine() {
            return deadLine;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOKFuture.complete(putMessageStatus);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        //存放putRequest方法写入的刷盘请求
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<GroupCommitRequest>();
        //存放doCommit方法读取的刷盘请求
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<GroupCommitRequest>();
        //同步服务锁
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        /**
         * GroupCommitService的方法
         * <p>
         * 加锁存入requestsWrite
         *
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            //获取锁
            lock.lock();
            try {
                //存入
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            //唤醒同步刷盘线程
            this.wakeup();
        }

        /**
         * GroupCommitService的方法
         * 交换请求
         */
        private void swapRequests() {
            //加锁
            lock.lock();
            try {
                //交换读写队列
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                //requestsRead是一个空队列
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        /**
         * GroupCommitService的方法
         * 执行同步刷盘操作
         */
        private void doCommit() {
            //如果requestsRead读队列不为空，表示有提交请求
            if (!this.requestsRead.isEmpty()) {
                //遍历所有的刷盘请求
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    //一个同步刷盘请求最多进行两次刷盘操作，因为文件是固定大小的，第一次刷盘时可能出现上一个文件剩余大小不足的情况
                    //消息只能再一次刷到下一个文件中，因此最多会出现两次刷盘的情况

                    //如果flushedWhere大于下一个刷盘点位，则表示该位置的数据已经刷刷盘成功了，不再需要刷盘
                    //flushedWhere的CommitLog的整体已刷盘物理偏移量
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    //最多循环刷盘两次
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        /*
                         * 执行强制刷盘操作，最少刷0页，即所有消息都会刷盘
                         */
                        CommitLog.this.mappedFileQueue.flush(0);
                        //判断是否刷盘成功，如果上一个文件剩余大小不足，则flushedWhere会小于nextOffset，那么海选哦再刷一次
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    }
                    //内部调用flushOKFuture.complete方法存入结果，将唤醒因为提交同步刷盘请求而被阻塞的线程
                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
                //获取存储时间戳
                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                //修改StoreCheckpoint中的physicMsgTimestamp：最新commitlog文件的刷盘时间戳，单位毫秒
                //这里用于重启数据恢复
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }
                //requestsRead重新创建一个空的队列，当下一次交换队列的时候，requestsWrite又会成为一个空队列
                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                //某些消息的设置是同步刷盘但是不等待，因此这里直接进行刷盘即可，无需唤醒线程等操作
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        /**
         * GroupCommitService的方法
         */
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            /*
             * 运行时逻辑
             * 如果服务没有停止，则在死循环中执行刷盘的操作
             */
            while (!this.isStopped()) {
                try {
                    //等待执行刷盘，固定每10ms执行一次
                    this.waitForRunning(10);
                    //尝试执行批量刷盘
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            /*
             * 停止时逻辑
             * 在正常情况下服务关闭时，将会线程等待10ms等待请求到达，然后一次性将剩余的request进行刷盘。
             */
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn(this.getServiceName() + " Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        /**
         * GroupCommitService交换读写队列
         */
        @Override
        protected void onWaitEnd() {
            //交换请求
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        private final ByteBuffer msgIdV6Memory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        /**
         * DefaultAppendMessageCallback的方法
         * <p>
         * 追加消息回调
         *
         * @param fileFromOffset    文件起始索引
         * @param byteBuffer        缓冲区
         * @param maxBlank          最大空闲区
         * @param msgInner          消息
         * @param putMessageContext 上下文
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            //获取物理偏移量索引
            long wroteOffset = fileFromOffset + byteBuffer.position();
            /*
             * 构建msgId,也就是broker端的唯一id,在发送消息的时候,在客户端producer也会生成一个唯一id是的。
             */
            Supplier<String> msgIdSupplier = () -> {
                //系统标识
                int sysflag = msgInner.getSysFlag();
                //长度16
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                //分配16字节的缓冲区
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                //ip4个字节、host4个字节
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                //清除缓冲区,因为因为socketAddress2ByteBuffer会翻转缓冲区
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                //8个字节存储commitLog的物理偏移量
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            //记录ConsumeQueue信息

            //key = "topic-queueId"
            String key = putMessageContext.getTopicQueueTableKey();
            //获取该队列的最大相对偏移量
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                //如果为null则设置为0,并且存入topicQueueTable
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            //light message queue(LMQ)支持
            boolean multiDispatchWrapResult = CommitLog.this.multiDispatch.wrapMultiDispatch(msgInner);
            if (!multiDispatchWrapResult) {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            // Transaction messages that require special handling
            //需要特殊处理的事务消息
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                //准备和回滚消息不会被消费，不会进入消费队列
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                //非事务消息和提交消息会被消费
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }
            /*
             * 消息编码序列化
             */
            //获取编码的ByteBuffer
            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            final int msgLen = preEncodeBuffer.getInt(0);

            // Determines whether there is sufficient free space
            //确定是否有足够的空间
            //消息的长度加上文件结束符子节数大于maxBlank，则表示该commitlog剩余大小不足以存储该消息。

            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                //返回END_OF_FILE，将会新建一个MappedFile并尝试再次存储
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                        maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                        msgIdSupplier, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4 + 4 + 4 + 4 + 4;
            // 6 QUEUEOFFSET
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + ipLen;
            // refresh store time stamp in lock
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());

            //存储消息起始时间
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            /*
             * 将消息写入到byteBuffer中，这里的byteBuffer可能是writeBuffer，即直接缓冲区，也有可能是普通缓冲区mappedByteBuffer
             */
            byteBuffer.put(preEncodeBuffer);
            msgInner.setEncodedBuff(null);
            //返回AppendMessageResult，包括消息追加状态、消息写入物理偏移量、消息写入长度、消息ID生成器、消息开始追加的时间戳、消息队列偏移量、消息开始写入的时间戳
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                    msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    CommitLog.this.multiDispatch.updateMultiQueueOffset(msgInner);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            String key = putMessageContext.getTopicQueueTableKey();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtEncoder {
        // Store the message content
        private final ByteBuffer encoderBuffer;
        // The maximum length of the message
        private final int maxMessageSize;

        MessageExtEncoder(final int size) {
            this.encoderBuffer = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        private void socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            InetAddress address = inetSocketAddress.getAddress();
            if (address instanceof Inet4Address) {
                byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
            } else {
                byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
            }
            byteBuffer.putInt(inetSocketAddress.getPort());
        }

        protected PutMessageResult encode(MessageExtBrokerInner msgInner) {
            /**
             * Serialize message
             */
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }

            // Initialization of storage space
            this.resetByteBuffer(encoderBuffer, msgLen);
            // 1 TOTALSIZE
            this.encoderBuffer.putInt(msgLen);
            // 2 MAGICCODE
            this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.encoderBuffer.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.encoderBuffer.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.encoderBuffer.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET, need update later
            this.encoderBuffer.putLong(0);
            // 7 PHYSICALOFFSET, need update later
            this.encoderBuffer.putLong(0);
            // 8 SYSFLAG
            this.encoderBuffer.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.encoderBuffer.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            socketAddress2ByteBuffer(msgInner.getBornHost(), this.encoderBuffer);
            // 11 STORETIMESTAMP
            this.encoderBuffer.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            socketAddress2ByteBuffer(msgInner.getStoreHost(), this.encoderBuffer);
            // 13 RECONSUMETIMES
            this.encoderBuffer.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.encoderBuffer.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.encoderBuffer.putInt(bodyLength);
            if (bodyLength > 0)
                this.encoderBuffer.put(msgInner.getBody());
            // 16 TOPIC
            this.encoderBuffer.put((byte) topicLength);
            this.encoderBuffer.put(topicData);
            // 17 PROPERTIES
            this.encoderBuffer.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.encoderBuffer.put(propertiesData);

            encoderBuffer.flip();
            return null;
        }

        protected ByteBuffer encode(final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            encoderBuffer.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            // properties from MessageExtBatch
            String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
            final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
            int batchPropDataLen = batchPropData.length;
            if (batchPropDataLen > Short.MAX_VALUE) {
                CommitLog.log.warn("Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}.", batchPropDataLen, Short.MAX_VALUE);
                throw new RuntimeException("Properties size of messageExtBatch exceeded!");
            }
            final short batchPropLen = (short) batchPropDataLen;

            int batchSize = 0;
            while (messagesByteBuff.hasRemaining()) {
                batchSize++;
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);
                boolean needAppendLastPropertySeparator = propertiesLen > 0 && batchPropLen > 0
                        && messagesByteBuff.get(messagesByteBuff.position() - 1) != MessageDecoder.PROPERTY_SEPARATOR;

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                int totalPropLen = needAppendLastPropertySeparator ? propertiesLen + batchPropLen + 1
                        : propertiesLen + batchPropLen;
                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, totalPropLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.encoderBuffer.putInt(msgLen);
                // 2 MAGICCODE
                this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.encoderBuffer.putInt(bodyCrc);
                // 4 QUEUEID
                this.encoderBuffer.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.encoderBuffer.putInt(flag);
                // 6 QUEUEOFFSET
                this.encoderBuffer.putLong(0);
                // 7 PHYSICALOFFSET
                this.encoderBuffer.putLong(0);
                // 8 SYSFLAG
                this.encoderBuffer.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.encoderBuffer.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(bornHostHolder, bornHostLength);
                this.encoderBuffer.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                this.encoderBuffer.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(storeHostHolder, storeHostLength);
                this.encoderBuffer.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                this.encoderBuffer.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.encoderBuffer.putLong(0);
                // 15 BODY
                this.encoderBuffer.putInt(bodyLen);
                if (bodyLen > 0)
                    this.encoderBuffer.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.encoderBuffer.put((byte) topicLength);
                this.encoderBuffer.put(topicData);
                // 17 PROPERTIES
                this.encoderBuffer.putShort((short) totalPropLen);
                if (propertiesLen > 0) {
                    this.encoderBuffer.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                if (batchPropLen > 0) {
                    if (needAppendLastPropertySeparator) {
                        this.encoderBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
                    }
                    this.encoderBuffer.put(batchPropData, 0, batchPropLen);
                }
            }
            putMessageContext.setBatchSize(batchSize);
            putMessageContext.setPhyPos(new long[batchSize]);
            encoderBuffer.flip();
            return encoderBuffer;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

        public ByteBuffer getEncoderBuffer() {
            return encoderBuffer;
        }
    }

    static class PutMessageThreadLocal {
        private MessageExtEncoder encoder;
        private StringBuilder keyBuilder;

        PutMessageThreadLocal(int size) {
            encoder = new MessageExtEncoder(size);
            keyBuilder = new StringBuilder();
        }

        public MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }

    static class PutMessageContext {
        private String topicQueueTableKey;
        private long[] phyPos;
        private int batchSize;

        public PutMessageContext(String topicQueueTableKey) {
            this.topicQueueTableKey = topicQueueTableKey;
        }

        public String getTopicQueueTableKey() {
            return topicQueueTableKey;
        }

        public long[] getPhyPos() {
            return phyPos;
        }

        public void setPhyPos(long[] phyPos) {
            this.phyPos = phyPos;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }
}
