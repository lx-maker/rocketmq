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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    private final String storePath;

    protected final int mappedFileSize;

    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    private final AllocateMappedFileService allocateMappedFileService;

    protected long flushedWhere = 0;
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * MappedFileQueue的方法
     *
     * @param offset 文件的最大有效数据偏移量
     */
    public void truncateDirtyFiles(long offset) {
        //待移除的文件集合
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        //遍历内部所有的MappedFile文件
        for (MappedFile file : this.mappedFiles) {
            //获取当前文件自身的最大数据偏移量
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            //如果最大数据偏移量大于最大有效数据偏移量
            if (fileTailOffset > offset) {
                //如果最大有效数据偏移量大于等于该文件的起始偏移量，那么说明当前文件有一部分数据是有效的，那么设置该文件的有效属性
                if (offset >= file.getFileFromOffset()) {
                    //设置当前文件的刷盘、提交、写入指针为当前最大有效数据偏移量
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    //如果如果最大有效数据偏移量小于该文件的起始偏移量，那么删除该文件
                    file.destroy(1000);
                    //记录到待删除的文件集合中
                    willRemoveFiles.add(file);
                }
            }
        }
        //将等待移除的文件整体从mappedFiles中移除
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * MappedFileQueue的方法
     *
     * @param files 待移除的文件集合
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    //从mappedFiles集合中删除当前MappedFile
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                //如果并没有完全移除这些无效文件，那么记录异常信息
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * MappedFileQueue的方法
     */
    public boolean load() {
        //获取commitlog文件的存放目录，目录路径取自
        //broker.conf文件中配置的storePathCommitLog属性，默认为$HOME/store/commitlog/
        File dir = new File(this.storePath);
        //获取内部的文件集合
        File[] ls = dir.listFiles();
        if (ls != null) {
            //如果存在commitlog文件，那么进行加载
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    /**
     * MappedFileQueue的方法
     */
    public boolean doLoad(List<File> files) {
        // 对commitlog文件按照文件名生序排序
        files.sort(Comparator.comparing(File::getName));

        for (File file : files) {
            //校验文件实际大小是否等于预定的文件大小，如果不想等，则直接返回false，不再加载其他文件
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                /*
                 * 核心代码
                 * 每一个commitlog文件都创建一个对应的MappedFile对象
                 *
                 */
                MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                //将wrotePosition 、flushedPosition、committedPosition 默认设置为文件大小
                //当前文件所映射到的消息写入page cache的位置
                mappedFile.setWrotePosition(this.mappedFileSize);
                //刷盘的最新位置
                mappedFile.setFlushedPosition(this.mappedFileSize);
                //已提交的最新位置
                mappedFile.setCommittedPosition(this.mappedFileSize);
                //添加到MappedFileQueue内部的mappedFiles集合中
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 创建或者获取最新的MappedFile
     *
     * @param startOffset 起始offset
     * @param needCreate  是否创建
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        //从mappedFiles集合中获取最后一个MappedFile
        MappedFile mappedFileLast = getLastMappedFile();
        //如果为null，那么设置创建索引，默认为0，即新建的文件为第一个mappedFile文件，从0开始
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        //如果满了，那么设置新mappedFile文件的创建索引 = 上一个文件的起始索引（即文件名） + mappedFileSize
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        //如果需要创建新mappedFile，那么根据起始索引创建新的mappedFile
        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 创建commitlog文件，映射MappedFile
     *
     * @param createOffset 起始索引，即新文件的文件名
     */
    protected MappedFile tryCreateMappedFile(long createOffset) {
        //下一个文件路径 {storePathCommitLog}/createOffset，即文件名为createOffset，即起始物理offset
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        //下下一个文件路径 {storePathCommitLog}/createOffset+mappedFileSize，即文件名为createOffset + mappedFileSize，即起始offset
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        //真正创建文件
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 创建commitlog文件，映射MappedFile
     *
     * @param nextFilePath     要创建的下一个文件路径
     * @param nextNextFilePath 要创建的下下一个文件路径
     */
    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;
        //如果allocateMappedFileService不为null，那么异步的创建MappedFile
        //CommitLog的MappedFileQueue初始化时会初始化allocateMappedFileService，因此一般都不为null
        if (this.allocateMappedFileService != null) {
            //添加两个请求到处理任务池，然后阻塞等待异步创建默认1G大小的MappedFile
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                //同步创建MappedFile
                mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            //如果是第一次创建，那么设置标志位firstCreateInQueue为true
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            //将创建的mappedFile加入mappedFiles集合中
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 创建新的MappedFile
     *
     * @param startOffset 指定起始offset
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 获取最新的MappedFile
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                //从mappedFiles中获取最后一个mappedFile
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 执行刷盘
     *
     * @param flushLeastPages 最少刷盘的页数
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        //根据最新刷盘物理位置flushedWhere，去找到对应的MappedFile。如果flushedWhere为0，表示还没有开始写消息，则获取第一个MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            //获取存储时间戳，storeTimestamp在appendMessagesInner方法中被更新
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            /*
             * 执行刷盘操作
             */
            int offset = mappedFile.flush(flushLeastPages);
            //获取最新刷盘物理偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            //刷盘结果
            result = where == this.flushedWhere;
            //更新刷盘物理位置
            this.flushedWhere = where;
            //如果最少刷盘页数为0，则更新存储时间戳
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 提交刷盘
     *
     * @param commitLeastPages 最少提交的页数
     * @return false表示提交了部分数据
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        //根据最新提交物理位置committedWhere，去找到对应的MappedFile。如果committedWhere为0，表示还没有开始提交消息，则获取第一个MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            /*
             * 执行提交操作
             */
            int offset = mappedFile.commit(commitLeastPages);
            //获取最新提交物理偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            //如果不相等，表示提交了部分数据
            result = where == this.committedWhere;
            //更新提交物理位置
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * MappedFileQueue的方法
     * <p>
     * 根据偏移量获取MappedFile
     *
     * @param offset                偏移量.
     * @param returnFirstOnNotFound 如果未找到映射文件，则返回第一个文件。
     * @return MappedFile 或者 null (当未找到且returnFirstOnNotFound为false时).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            //获取第一个MappedFile
            MappedFile firstMappedFile = this.getFirstMappedFile();
            //获取最后一个MappedFile
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                //如果偏移量不再正确的范围内，则打印异常日志
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                } else {
                    //获取当前offset属于的MappedFile在mappedFiles集合中的索引位置
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        //根据索引位置获取对应的MappedFile文件
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    //如果指定offset在targetFile的offset范围内，那么返回
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    //否则，遍历mappedFiles，依次对每个MappedFile的offset范围进行判断，找到对应的tmpMappedFile并返回
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                //到这里表示没找到任何MappedFile，如果returnFirstOnNotFound为true，则返回第一个文件
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
