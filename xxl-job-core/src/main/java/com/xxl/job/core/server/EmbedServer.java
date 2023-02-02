package com.xxl.job.core.server;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.ThrowableUtil;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Copy from : https://github.com/xuxueli/xxl-rpc
 *
 * @author xuxueli 2020-04-11 21:25
 */
public class EmbedServer {
    
    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);
    
    private ExecutorBiz executorBiz;
    
    private Thread thread;
    
    // 看到这个start,我们从全局上看,发现这个start方法就开启了一个新的线程, 那么这个线程做什么了呢?
    // 我们还需要关系它的run方法.
    public void start(final String address, final int port, final String appname, final String accessToken) {
        executorBiz = new ExecutorBizImpl();
        thread = new Thread(new Runnable() {
            
            @Override
            public void run() {
                // param
                // 下面这两个BossGroup和WorkerGroup是netty的模板写法,我们先不关心.
                // 可以自行去了解netty. 这里不着重讲这个.
                EventLoopGroup bossGroup = new NioEventLoopGroup();
                EventLoopGroup workerGroup = new NioEventLoopGroup();
                
                // 看到这里,我们发现作者又搞了一个线程池, 那么这个线程池是干什么用的呢?? 我们还不知道.
                // 不过别着急. 后面看着看着就了解了.
                ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(0, 200, 60L, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(2000), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-rpc, EmbedServer bizThreadPool-" + r.hashCode());
                    }
                }, new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
                    }
                });
                
                try {
                    // start server
                    // 下面这里就是开始启动netty server了, 都是模板的写法.
                    // 我们后面可以去关注, 这里的Handler
                    // 但是我们在下面的代码中, 发现了 startRegistry(appname, address); 这样一段.
                    // 看名字好像是要开始注册了.
                    ServerBootstrap bootstrap = new ServerBootstrap();
                    bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel channel) throws Exception {
                                    channel.pipeline().addLast(new IdleStateHandler(0, 0, 30 * 3,
                                            TimeUnit.SECONDS))  // beat 3N, close if idle
                                            .addLast(new HttpServerCodec()).addLast(new HttpObjectAggregator(
                                            5 * 1024 * 1024))  // merge request & reponse to FULL
                                            .addLast(new EmbedHttpServerHandler(executorBiz, accessToken,
                                                    bizThreadPool));
                                }
                            }).childOption(ChannelOption.SO_KEEPALIVE, true);
                    
                    // bind
                    ChannelFuture future = bootstrap.bind(port).sync();
                    
                    logger.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}",
                            EmbedServer.class, port);
                    
                    // start registry
                    // 这里开始注册.
                    startRegistry(appname, address);
                    
                    // wait util stop
                    future.channel().closeFuture().sync();
                    
                } catch (InterruptedException e) {
                    if (e instanceof InterruptedException) {
                        logger.info(">>>>>>>>>>> xxl-job remoting server stop.");
                    } else {
                        logger.error(">>>>>>>>>>> xxl-job remoting server error.", e);
                    }
                } finally {
                    // stop
                    try {
                        workerGroup.shutdownGracefully();
                        bossGroup.shutdownGracefully();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                
            }
            
        });
        thread.setDaemon(true);    // daemon, service jvm, user thread leave >>> daemon leave >>> jvm leave
        thread.start();
    }
    
    public void stop() throws Exception {
        // destroy server thread
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }
        
        // stop registry
        stopRegistry();
        logger.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
    }
    
    // ---------------------- registry ----------------------
    
    /**
     * netty_http
     * <p>
     * Copy from : https://github.com/xuxueli/xxl-rpc
     *
     * @author xuxueli 2015-11-24 22:25:15
     */
    // 看这个类的注释我们也看到了, 这里使用了netty http. 这个FullHttpRequest也是netty的.
    // 我们也就主要看 channelRead0 就好了. 我们在很多源码中会看到类似 xx0 doXxx doXxx0这样的方法. 其实这些方法都比较重要.
    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        
        private static final Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);
        
        private ExecutorBiz executorBiz;
        
        private String accessToken;
        
        private ThreadPoolExecutor bizThreadPool;
        
        public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
            this.executorBiz = executorBiz;
            this.accessToken = accessToken;
            this.bizThreadPool = bizThreadPool;
        }
    
        /**
         * 这个channelRead0 可以简单的理解为执行器作为netty server 接收到了 调度中心发送的请求.
         * 那么调度中心发送请求给执行器做什么呢?? 我们之前在调度中心的代码也看到了, 就是通知执行器执行任务.
         * @param ctx
         * @param msg
         * @throws Exception
         */
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            // 从整体上看这个方法也比较简单, 就是 拿出请求数据, 丢给线程池去执行任务.
            // 我们就直接去看run方法好了.
            
            // request parse
            //final byte[] requestBytes = ByteBufUtil.getBytes(msg.content());    // byteBuf.toString(io.netty.util.CharsetUtil.UTF_8);
            String requestData = msg.content().toString(CharsetUtil.UTF_8);
            String uri = msg.uri();
            HttpMethod httpMethod = msg.method();
            boolean keepAlive = HttpUtil.isKeepAlive(msg);
            String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);
            
            // invoke
            // 这个bizThreadPool 是何时初始化的??  实际上就是在开始新线程启动netty server前初始化了这个线程池.
            bizThreadPool.execute(new Runnable() {
                // 这个run呢也比较清晰, 1.invoke 2.tojson 3.writeResponse.
                // 我们就先看看作者是如何invoke的吧. 实际上呢想想可能也挺简单, 要知道执行器在启动阶段已经搜集了@Xxl-job注解方法.
                @Override
                public void run() {
                    // do invoke
                    Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);
                    
                    // to json
                    String responseJson = GsonTool.toJson(responseObj);
                    
                    // write response
                    writeResponse(ctx, keepAlive, responseJson);
                }
            });
        }
        
        // 处理请求.
        private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
            
            // valid
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }
            if (uri == null || uri.trim().length() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }
            if (accessToken != null && accessToken.trim().length() > 0 && !accessToken.equals(accessTokenReq)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }
            
            // services mapping
            // 这里有一堆if 判断, 我们执行在调度中心看到, 调度中心在通知执行器执行任务时是哪个请求??
            // 是不是/run请求?? 那么我们先看看run 到底干什么了.
            try {
                if ("/beat".equals(uri)) {
                    return executorBiz.beat();
                } else if ("/idleBeat".equals(uri)) {
                    IdleBeatParam idleBeatParam = GsonTool.fromJson(requestData, IdleBeatParam.class);
                    return executorBiz.idleBeat(idleBeatParam);
                } else if ("/run".equals(uri)) {
                    // 这一步就直接反序列化json了. 对吧. 那这个TriggerParam 是哪来的??
                    // 调度中心 是不是调用run的时候就是传入的这个TriggerParam.
                    TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                    // 执行.
                    // 这个executor 也是一个接口, 按照我们刚才的经验, 那这里是不是应该就是ExecutorBizClient
                    // 但是实际上并不是啊. 其实是ExecutorBizImpl.
                    return executorBiz.run(triggerParam);
                } else if ("/kill".equals(uri)) {
                    KillParam killParam = GsonTool.fromJson(requestData, KillParam.class);
                    return executorBiz.kill(killParam);
                } else if ("/log".equals(uri)) {
                    LogParam logParam = GsonTool.fromJson(requestData, LogParam.class);
                    return executorBiz.log(logParam);
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            "invalid request, uri-mapping(" + uri + ") not found.");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }
        
        /**
         * write response
         */
        private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
            // write response
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));   //  Unpooled.wrappedBuffer(responseJson)
            response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                    "text/html;charset=UTF-8");       // HttpHeaderValues.TEXT_PLAIN.toString()
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            ctx.writeAndFlush(response);
        }
        
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
            ctx.close();
        }
        
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();      // beat 3N, close if idle
                logger.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }
    
    // ---------------------- registry ----------------------
    public void startRegistry(final String appname, final String address) {
        // 这里开始注册,我们发现作者搞了个单例, 而且看名字又和Thread有关系,
        // 所以我们猜测是不是又搞了个新线程去注册?? 那么为什么又要搞新线程呢??其实我个人看到这里的猜测就是要使用这个线程完成心跳.
        // 要不然光注册,不通知调度中心更新update_time, 那岂不是会超时被delete掉嘛.
        // 我们去看看好了.
        // start registry
        ExecutorRegistryThread.getInstance().start(appname, address);
    }
    
    public void stopRegistry() {
        // stop registry
        ExecutorRegistryThread.getInstance().toStop();
    }
    
    
}
