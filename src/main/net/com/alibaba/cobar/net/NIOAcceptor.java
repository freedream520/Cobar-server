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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.cobar.net.factory.FrontendConnectionFactory;

/**
 * @author xianmao.hexm
 * Annotation by MingYan
 * 该类用于接受来自客户端的连接
 */
public final class NIOAcceptor extends Thread {
    private static final Logger LOGGER = Logger.getLogger(NIOAcceptor.class);
    private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

    private final int port;
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;
    private NIOProcessor[] processors;
    private int nextProcessor;
    private long acceptCount;

    //构造函数主要完成:
    //获取selector,获取serverChannel,绑定监听的端口,设置Channel为非阻塞,向selector注册该channel
    //并绑定感兴趣的事件为OP_ACCEPT
    public NIOAcceptor(String name, int port, FrontendConnectionFactory factory) throws IOException {
        super.setName(name);
        this.port = port;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        //ServerSocket使用TCP
        this.serverChannel.socket().bind(new InetSocketAddress(port));
        this.serverChannel.configureBlocking(false);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.factory = factory;
    }

    public int getPort() {
        return port;
    }

    public long getAcceptCount() {
        return acceptCount;
    }

    public void setProcessors(NIOProcessor[] processors) {
        this.processors = processors;
    }

    @Override
    public void run() {
        final Selector selector = this.selector;
        //线程一直循环
        for (;;) {
            ++acceptCount;
            try {
                selector.select(1000L);
                Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                        	//接受来自客户端的连接
                            accept();
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                LOGGER.warn(getName(), e);
            }
        }
    }

    private void accept() {
        SocketChannel channel = null;
        try {
        	//从服务器端获取管道,为一个新的连接返回channel
            channel = serverChannel.accept();
            //配置管道为非阻塞
            channel.configureBlocking(false);
            
            //前端连接工厂对管道进行配置，设置socket的收发缓冲区大小，TCP延迟等
            //然后由成员变量factory的类型生产对于的类型的连接
            //比如ServerConnectionFactory会返回ServerConnection实例，并对其属性进行设置
            FrontendConnection c = factory.make(channel);
            //设置连接属性
            c.setAccepted(true);
            c.setId(ID_GENERATOR.getId());
            //从processors中选择一个NIOProcessor，将其和该连接绑定
            NIOProcessor processor = nextProcessor();
            c.setProcessor(processor);
            //向读反应堆注册该连接，加入待处理队列
            //select选择到感兴趣的事件后，会进行调用connection的read函数
            processor.postRegister(c);
        } catch (Throwable e) {
            closeChannel(channel);
            LOGGER.warn(getName(), e);
        }
    }

    private NIOProcessor nextProcessor() {
        if (++nextProcessor == processors.length) {
            nextProcessor = 0;
        }
        return processors[nextProcessor];
    }

    private static void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
        }
    }

    /**
     * 前端连接ID生成器
     * 
     * @author xianmao.hexm
     */
    private static class AcceptIdGenerator {

        private static final long MAX_VALUE = 0xffffffffL;

        private long acceptId = 0L;
        private final Object lock = new Object();

        private long getId() {
            synchronized (lock) {
                if (acceptId >= MAX_VALUE) {
                    acceptId = 0L;
                }
                return ++acceptId;
            }
        }
    }

}
