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
package com.alibaba.cobar;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;

import com.alibaba.cobar.config.model.SystemConfig;
import com.alibaba.cobar.manager.ManagerConnectionFactory;
import com.alibaba.cobar.mysql.MySQLDataNode;
import com.alibaba.cobar.net.NIOAcceptor;
import com.alibaba.cobar.net.NIOConnector;
import com.alibaba.cobar.net.NIOProcessor;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.cobar.server.ServerConnectionFactory;
import com.alibaba.cobar.statistic.SQLRecorder;
import com.alibaba.cobar.util.ExecutorUtil;
import com.alibaba.cobar.util.NameableExecutor;
import com.alibaba.cobar.util.TimeUtil;

/**
 * @author xianmao.hexm 2011-4-19 下午02:58:59
 */
public class CobarServer {
	
	//static final表示全局常量
    public static final String NAME = "Cobar";
    private static final long LOG_WATCH_DELAY = 60000L;
    private static final long TIME_UPDATE_PERIOD = 20L;
    
    //static变量只初始化一次,单例模式
    private static final CobarServer INSTANCE = new CobarServer();
    private static final Logger LOGGER = Logger.getLogger(CobarServer.class);

    public static final CobarServer getInstance() {
        return INSTANCE;
    }

    private final CobarConfig config;
    private final Timer timer;
    
    //NameableExecutor是继承自ThreadPoolExecuter的命名线程池
    //managerExecutor处理Web管理端的请求
    //timerExecutor是处理定时执行任务
    //
    private final NameableExecutor managerExecutor;
    private final NameableExecutor timerExecutor;
    private final NameableExecutor initExecutor;
    
    private final SQLRecorder sqlRecorder;
    private final AtomicBoolean isOnline;
    private final long startupTime;
    private NIOProcessor[] processors;
    private NIOConnector connector;
    private NIOAcceptor manager;
    private NIOAcceptor server;

    private CobarServer() {
        this.config = new CobarConfig();
        SystemConfig system = config.getSystem();
        MySQLLexer.setCStyleCommentVersion(system.getParserCommentVersion());
        this.timer = new Timer(NAME + "Timer", true);
        
        //创建命名线程池，可以daemon运行，实现接口ThreadPoolExecutor
        this.initExecutor = ExecutorUtil.create("InitExecutor", system.getInitExecutor());
        this.timerExecutor = ExecutorUtil.create("TimerExecutor", system.getTimerExecutor());
        this.managerExecutor = ExecutorUtil.create("ManagerExecutor", system.getManagerExecutor());
        
        //创建SQL统计排序记录器对象
        this.sqlRecorder = new SQLRecorder(system.getSqlRecordCount());
        this.isOnline = new AtomicBoolean(true);
        this.startupTime = TimeUtil.currentTimeMillis();
    }

    public CobarConfig getConfig() {
        return config;
    }

    //初始化日志系统
    public void beforeStart(String dateFormat) {
        String home = System.getProperty("cobar.home");
        if (home == null) {
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            LogLog.warn(sdf.format(new Date()) + " [cobar.home] is not set.");
        } else {
            Log4jInitializer.configureAndWatch(home + "/conf/log4j.xml", LOG_WATCH_DELAY);
        }
    }

    public void startup() throws IOException {
        // server startup
        LOGGER.info("===============================================");
        LOGGER.info(NAME + " is ready to startup ...");
        SystemConfig system = config.getSystem();
        
        //TODO 定时刷新时间，为何？需要的时候可以直接获取
        timer.schedule(updateTime(), 0L, TIME_UPDATE_PERIOD);

        // startup processors
        LOGGER.info("Startup processors ...");
        //得到处理器核心数
        int handler = system.getProcessorHandler();
        int executor = system.getProcessorExecutor();
        
        //按处理器核心个数新建NIO处理，这样在一定程度上可以做到业务的隔离，一个processor处理的
        //任务不会影响其他的processor任务
        //每个processor的handler和executer是继承自ThreadPoolExecutor的
        processors = new NIOProcessor[system.getProcessors()];
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new NIOProcessor("Processor" + i, handler, executor);
            //每个processor都启动该processor的读和写reactor线程
            processors[i].startup();
        }
        
        //定时执行检查任务，回收资源，分别遍历前端和后端连接的ConcurrentHashmap,移除失效的连接
        //清理已经关闭连接，并进行资源回收，否则对连接进行超时检查
        timer.schedule(processorCheck(), 0L, system.getProcessorCheckPeriod());

        // startup connector
        LOGGER.info("Startup connector ...");
        //NIOConnecotr继承了Thread类，start启动线程
        //TODO 处理后端连接,向后端MySQL节点建立连接
        connector = new NIOConnector(NAME + "Connector");
        connector.setProcessors(processors);//设置NIOProcessor
        connector.start();

        // init dataNodes
        Map<String, MySQLDataNode> dataNodes = config.getDataNodes();
        LOGGER.info("Initialize dataNodes ...");
        for (MySQLDataNode node : dataNodes.values()) {
            node.init(1, 0);
        }
        //数据节点定时连接空闲超时检查任务
        timer.schedule(dataNodeIdleCheck(), 0L, system.getDataNodeIdleCheckPeriod());
        //数据节点心跳发送任务
        timer.schedule(dataNodeHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod());

        // startup manager
        // TODO 先不分析前端部分
//        ManagerConnectionFactory mf = new ManagerConnectionFactory();
//        mf.setCharset(system.getCharset());
//        mf.setIdleTimeout(system.getIdleTimeout());
//        manager = new NIOAcceptor(NAME + "Manager", system.getManagerPort(), mf);
//        manager.setProcessors(processors);
//        manager.start();
//        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());

        // startup server
        //创建服务器连接工厂
        ServerConnectionFactory sf = new ServerConnectionFactory();
        sf.setCharset(system.getCharset());
        sf.setIdleTimeout(system.getIdleTimeout());
        
        //下面创建的NIOAcceptor用于接收客户端连接
        //构造函数完成获取selector，建立ServerSocketChannel建立，绑定端口
        //设置channel为非阻塞，向selector注册该channel
        //name port serverconnectionfactory
        server = new NIOAcceptor(NAME + "Server", system.getServerPort(), sf);
        server.setProcessors(processors);
        server.start();
        
        //向Cobar集群发送心跳包
        timer.schedule(clusterHeartbeat(), 0L, system.getClusterHeartbeatPeriod());

        // server started
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());
        LOGGER.info("===============================================");
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public NIOConnector getConnector() {
        return connector;
    }

    public NameableExecutor getManagerExecutor() {
        return managerExecutor;
    }

    public NameableExecutor getTimerExecutor() {
        return timerExecutor;
    }

    public NameableExecutor getInitExecutor() {
        return initExecutor;
    }

    public SQLRecorder getSqlRecorder() {
        return sqlRecorder;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    // 系统时间定时更新任务
    private TimerTask updateTime() {
        return new TimerTask() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    // 处理器定时检查任务
    private TimerTask processorCheck() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                    	LOGGER.debug("处理器定时检查任务");
                        for (NIOProcessor p : processors) {
                            p.check();
                        }
                    }
                });
            }
        };
    }

    // 数据节点定时连接空闲超时检查任务
    private TimerTask dataNodeIdleCheck() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                    	LOGGER.debug("数据节点定时连接空闲超时检查");
                        Map<String, MySQLDataNode> nodes = config.getDataNodes();
                        for (MySQLDataNode node : nodes.values()) {
                            node.idleCheck();
                        }
                        Map<String, MySQLDataNode> _nodes = config.getBackupDataNodes();
                        if (_nodes != null) {
                            for (MySQLDataNode node : _nodes.values()) {
                                node.idleCheck();
                            }
                        }
                    }
                });
            }
        };
    }

    // 数据节点定时心跳任务
    private TimerTask dataNodeHeartbeat() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                    	LOGGER.debug("数据节点定时心跳任务");
                        Map<String, MySQLDataNode> nodes = config.getDataNodes();
                        for (MySQLDataNode node : nodes.values()) {
                            node.doHeartbeat();
                        }
                    }
                });
            }
        };
    }

    // 集群节点定时心跳任务
    private TimerTask clusterHeartbeat() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                    	LOGGER.debug("集群节点定时心跳任务");
                        Map<String, CobarNode> nodes = config.getCluster().getNodes();
                        for (CobarNode node : nodes.values()) {
                            node.doHeartbeat();
                        }
                    }
                });
            }
        };
    }

}
