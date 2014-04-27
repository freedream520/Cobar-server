/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cobar.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.alibaba.cobar.config.ErrorCode;

/**
 * 网络事件反应器
 * 
 * @author xianmao.hexm
 */
public final class NIOReactor {
    private static final Logger LOGGER = Logger.getLogger(NIOReactor.class);

    private final String name;
    private final R reactorR;
    private final W reactorW;

    public NIOReactor(String name) throws IOException {
        this.name = name;
        this.reactorR = new R();
        this.reactorW = new W();
    }

    final void startup() {
    	//启动两个线程
        new Thread(reactorR, name + "-R").start();
        new Thread(reactorW, name + "-W").start();
    }

    final void postRegister(NIOConnection c) {
    	//先将连接加入队列,然后唤醒
        reactorR.registerQueue.offer(c);
        reactorR.selector.wakeup();
    }

    final BlockingQueue<NIOConnection> getRegisterQueue() {
        return reactorR.registerQueue;
    }

    final long getReactCount() {
        return reactorR.reactCount;
    }

    final void postWrite(NIOConnection c) {
    	//将要要发送的连接(内容在连接对象中的缓冲区队列中)加入队列
    	//reactorW会按序查看队列,进行发送
        reactorW.writeQueue.offer(c);
    }

    final BlockingQueue<NIOConnection> getWriteQueue() {
        return reactorW.writeQueue;
    }

    private final class R implements Runnable {
        private final Selector selector;
        private final BlockingQueue<NIOConnection> registerQueue;
        private long reactCount;

        private R() throws IOException {
            this.selector = Selector.open();
            this.registerQueue = new LinkedBlockingQueue<NIOConnection>();
        }

        @Override
        public void run() {
            final Selector selector = this.selector;
            for (;;) {
                ++reactCount;
                try {
                	//TODO 为何设置select超时时间
                    int res = selector.select();
                    LOGGER.debug(reactCount + ">>NIOReactor接受连接数:" + res);
                    register(selector);
                    Set<SelectionKey> keys = selector.selectedKeys();
                    try {
                        for (SelectionKey key : keys) {
                            Object att = key.attachment();
                            if (att != null && key.isValid()) {
                                int readyOps = key.readyOps();
                                if ((readyOps & SelectionKey.OP_READ) != 0) {
                                	LOGGER.debug("select读事件");
                                    read((NIOConnection) att);
                                } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                                	LOGGER.debug("select写事件");
                                    write((NIOConnection) att);
                                } else {
                                    key.cancel();
                                }
                            } else {
                                key.cancel();
                            }
                        }
                    } finally {
                        keys.clear();
                    }
                } catch (Throwable e) {
                    LOGGER.warn(name, e);
                }
            }
        }

        private void register(Selector selector) {
            NIOConnection c = null;
            //将待注册队列中的连接向selector注册
            while ((c = registerQueue.poll()) != null) {
                try {
                	//该函数定义在接口NIOConnection
                	//具体实现在其子类FrontendConnection和BackendConnection中
                	//eg.在FrontendConnection实现的函数中会:
                	//1 将channel向selector注册OP_READ读事件
                	//2 向连接的客户端发送握手包
                    c.register(selector);
                } catch (Throwable e) {
                    c.error(ErrorCode.ERR_REGISTER, e);
                }
            }
        }

        private void read(NIOConnection c) {
            try {
                c.read();
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_READ, e);
            }
        }

        private void write(NIOConnection c) {
            try {
                c.writeByEvent();
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_WRITE_BY_EVENT, e);
            }
        }
    }

    private final class W implements Runnable {
        private final BlockingQueue<NIOConnection> writeQueue;

        private W() {
            this.writeQueue = new LinkedBlockingQueue<NIOConnection>();
        }

        @Override
        public void run() {
        	//客户端发送认证信息,前端将包含回复信息的链接加入队列,按顺序发送
            NIOConnection c = null;
            for (;;) {
                try {
                    if ((c = writeQueue.take()) != null) {
                        write(c);
                    }
                } catch (Throwable e) {
                    LOGGER.warn(name, e);
                }
            }
        }

        private void write(NIOConnection c) {
            try {
            	//TODO 调用具体连接的writeByQueue函数进行数据的发送
            	//具体发送过程待探究
                c.writeByQueue();
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_WRITE_BY_QUEUE, e);
            }
        }
    }

}
