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
package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 事务消息回查服务
 */
public class TransactionalMessageCheckService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    public TransactionalMessageCheckService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return TransactionalMessageCheckService.class.getSimpleName();
    }

    /**
     * TransactionalMessageCheckService的方法
     */
    @Override
    public void run() {
        log.info("Start transaction check service thread!");
        //获取事务回查时间间隔，默认60s，可通过broker.conf配置transactionCheckInterval属性更改
        long checkInterval = brokerController.getBrokerConfig().getTransactionCheckInterval();
        //循环回查
        while (!this.isStopped()) {
            //最多等待60s执行一次回查
            this.waitForRunning(checkInterval);
        }
        log.info("End transaction check service thread!");
    }

    /**
     * 被唤醒或者等待时间到了之后，执行事务回查
     */
    @Override
    protected void onWaitEnd() {
        //事务超时时间，默认6s，即超过6s还没有被commit或者rollback的事物消息将会进行回查，可通过broker.conf配置transactionTimeOut属性更改
        long timeout = brokerController.getBrokerConfig().getTransactionTimeOut();
        //事务回查最大次数，默认15，超过次数则丢弃消息，可通过broker.conf配置transactionCheckMax属性更改
        int checkMax = brokerController.getBrokerConfig().getTransactionCheckMax();
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);
        //执行事务回查
        this.brokerController.getTransactionalMessageService().check(timeout, checkMax, this.brokerController.getTransactionalMessageCheckListener());
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }

}
