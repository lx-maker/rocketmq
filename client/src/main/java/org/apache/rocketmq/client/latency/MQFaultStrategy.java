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

package org.apache.rocketmq.client.latency;

import javafx.scene.shape.Path;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.file.Files;
import java.nio.file.Paths;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;


    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * MQFaultStrategy的方法
     * <p>
     * 选择一个消息队列，支持故障延迟转移
     *
     * @param tpInfo         topic信息
     * @param lastBrokerName 上次使用过的broker
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        /*
         * 判断是否开启了发送延迟故障转移机制，默认false不打开
         * 如果开启了该机制，那么每次选取topic下对应的queue时，会基于之前执行的耗时，在有存在符合条件的broker的前提下，优选选取一个延迟较短的broker，否则再考虑随机选取。
         */
        if (this.sendLatencyFaultEnable) {
            try {
                //当前线程线程的消息队列的下标，循环选择消息队列使用+1
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                //遍历消息队列，采用取模的方式获取一个队列，即轮询的方式
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    //取模
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //获取该消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //如果当前消息队列是可用的，即无故障，那么直接返回该mq
                    //如果该broker不存在LatencyFaultTolerance维护的faultItemTable集合属性中，或者当前时间已经大于该broker下一次开始可用的时间点，表示无故障
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                //没有选出无故障的mq，那么一个不是最好的broker集合中随机选择一个
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //如果写队列数大于0，那么选择该broker
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    //遍历消息队列，采用取模的方式获取一个队列，即轮询的方式
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        //重置其brokerName，queueId，进行消息发送
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //如果写队列数小于0，那么移除该broker
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //如果上面的步骤抛出了异常，那么遍历消息队列，采用取模的方式获取一个队列，即轮询的方式
            return tpInfo.selectOneMessageQueue();
        }
        //如果没有发送延迟故障转移机制，那么那么遍历消息队列，即采用取模轮询的方式
        //获取一个brokerName与lastBrokerName不相等的队列，即不会再次选择上次发送失败的broker
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * MQFaultStrategy的方法
     *
     * @param brokerName     brokerName
     * @param currentLatency 当前延迟
     * @param isolation      是否使用默认隔离时间
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //如果开启了故障转移，即sendLatencyFaultEnable为true，默认false
        if (this.sendLatencyFaultEnable) {
            //根据消息当前延迟currentLatency计算当前broker的故障延迟的时间duration
            //如果isolation为true，则使用默认隔离时间30000，即30s
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //更新故障记录表
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }

    }

    //延迟等级
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    //不可用时间等级
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    /**
     * MQFaultStrategy的方法
     *
     * @param currentLatency 当前延迟
     * @return 故障延迟的时间
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        //倒叙遍历latencyMax
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            //选择broker延迟时间对应的broker不可用时间，默认30000对应的故障延迟的时间为600000，即10分钟
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
