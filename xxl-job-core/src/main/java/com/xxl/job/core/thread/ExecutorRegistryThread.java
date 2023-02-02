package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by xuxueli on 17/3/2.
 */
public class ExecutorRegistryThread {
    private static Logger logger = LoggerFactory.getLogger(ExecutorRegistryThread.class);

    private static ExecutorRegistryThread instance = new ExecutorRegistryThread();
    public static ExecutorRegistryThread getInstance(){
        return instance;
    }

    private Thread registryThread;
    private volatile boolean toStop = false;
    
    // 方法收起来我们发现,这个方法还真开启了一个新线程
    // 猜测的很对啊, 那么我们就关心一下run方法中到底做了什么呢.
    public void start(final String appname, final String address){

        // valid
        // 这里的if逻辑我们就直接过掉了啊.
        if (appname==null || appname.trim().length()==0) {
            logger.warn(">>>>>>>>>>> xxl-job, executor registry config fail, appname is null.");
            return;
        }
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, executor registry config fail, adminAddresses is null.");
            return;
        }

        registryThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // registry
                // 看到没有这里又有一个接近于自旋的线程. 至于何时设置toStop=true,我们也先不关心.
                while (!toStop) {
                    try {
                        // 这里实例化了一个RegistryParam对象
                        // 然后遍历了调度中心列表, 注意这里为什么是调度中心列表?? 因为调度中心可以有多个.
                        // 那么调度中心的地址我们配置的是字符串逗号拼接啊, 怎么变成List类型了??
                        // 实际上是 XxlJobExecutor类在开启NettyServer前, 调用initAdminBizList方法做的.
                        RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
                        for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
                            try {
                                // 看到这个adminBiz.registry方法,我们就很清楚啦. 这是要注册啦.
                                // 我们先不管它是怎么注册的,我们先整体看看下面还有没有什么逻辑了...
                                // 好了,下面注销逻辑看完了,我们关心一下执行器是怎么调用调度中心的.
                                // 我们发现AdminBiz是个接口,它有两个实现类, 那么这里使用的是哪个呢?? 我们就要去XxlJobExecutor.initAdminBizList方法中去看, List中放入的是哪个实现了.
                                // 我们看到其实是AdminBizClient. 那么我们就去看它的registry方法呗.
                                ReturnT<String> registryResult = adminBiz.registry(registryParam);
                                // if逻辑不看.
                                if (registryResult!=null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                    registryResult = ReturnT.SUCCESS;
                                    logger.debug(">>>>>>>>>>> xxl-job registry success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                                    break;
                                } else {
                                    logger.info(">>>>>>>>>>> xxl-job registry fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                                }
                            } catch (Exception e) {
                                logger.info(">>>>>>>>>>> xxl-job registry error, registryParam:{}", registryParam, e);
                            }

                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }

                    }

                    // 看到这里,我们发现还有一段, 就是每执行一次就sleep一段时间.
                    // 是不是和调度中心的启动时那个处理JobRegistry的线程逻辑如出一辙.
                    // sleep 30s.
                    try {
                        if (!toStop) {
                            TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                        }
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.warn(">>>>>>>>>>> xxl-job, executor registry thread interrupted, error msg:{}", e.getMessage());
                        }
                    }
                }

                // 看到这里我们发现, 这个while下面居然还有一段代码.
                // 那么这段代码是干啥的?? 从作者注释上看是要remove掉registry.
                // 也仅当toStop=true的时候才会执行下面这段代码.
                // 这段代码也很简单, 其实就是调用了调度中心的registryRemove方法就通知调度中心注销JobRegistry呗.
                // registry remove
                try {
                    RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
                    // 遍历所有的调度中心, 然后执行registryRemove.
                    // 我们就去看一下这个registryRemove就可以了.
                    for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
                        try {
                            // 这里我们在注册执行器的时候看过了, 这个adminBiz其实时AdminBizClient
                            ReturnT<String> registryResult = adminBiz.registryRemove(registryParam);
                            if (registryResult!=null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                registryResult = ReturnT.SUCCESS;
                                logger.info(">>>>>>>>>>> xxl-job registry-remove success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                                break;
                            } else {
                                logger.info(">>>>>>>>>>> xxl-job registry-remove fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                            }
                        } catch (Exception e) {
                            if (!toStop) {
                                logger.info(">>>>>>>>>>> xxl-job registry-remove error, registryParam:{}", registryParam, e);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor registry thread destory.");

            }
        });
        registryThread.setDaemon(true);
        registryThread.setName("xxl-job, executor ExecutorRegistryThread");
        registryThread.start();
    }

    public void toStop() {
        toStop = true;

        // interrupt and wait
        if (registryThread != null) {
            registryThread.interrupt();
            try {
                registryThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

    }

}
