package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * job registry instance
 *
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryHelper {
    
    private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);
    
    private static JobRegistryHelper instance = new JobRegistryHelper();
    
    public static JobRegistryHelper getInstance() {
        return instance;
    }
    
    private ThreadPoolExecutor registryOrRemoveThreadPool = null;
    
    private Thread registryMonitorThread;
    
    private volatile boolean toStop = false;
    
    /**
     * 从整体上看,这个start 就做了两件事情, 实例化了一个线程池. 启动了一个新线程.
     * <p>
     * 实例化线程池没什么好看的, 我们重点关注启动的这个线程干什么了.
     */
    public void start() {
        // 在这里初始化了一个线程池.
        // for registry or remove
        registryOrRemoveThreadPool = new ThreadPoolExecutor(2, 10, 30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,
                        "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode());
            }
        }, new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                r.run();
                logger.warn(
                        ">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
            }
        });
        
        // for monitor
        // 这里是实例化了一个线程, 下面会start. 那我们就看run方法就可以了.
        // 我们就看一看启动了这个线程到底干啥了呢??
        registryMonitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // 这里实际上相当于一个自旋. 直到有人设置toStop=false.
                while (!toStop) {
                    try {
                        // auto registry group
                        // 这里查询了XxlJobGroup列表,参数0. 那么实际查询的是什么??
                        // 我们可以去看他们sql脚本, 这个address_type执行器地址类型：0=自动注册、1=手动录入
                        List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao()
                                .findByAddressType(0);
                        
                        // 这里有一个if判断逻辑, 当我看这段代码时,我认为if里面的逻辑是不会执行的.因为我们初次使用xxl-job时. 肯定还没有任何的执行器.
                        // 然而.. 我在sql脚本中发现了居然初始化数据.
                        // 既然会执行if逻辑,那么我们就看看到底做什么了. 实话实说这个if里面的代码有点让我难以接受. 长长长.....
                        // 为什么就不能单独抽个方法出来呢?? 蛋疼.
                        if (groupList != null && !groupList.isEmpty()) {
                            // remove dead address (admin/executor)
                            // 看下面这段代码,不难看出, 又到xxl_job_registry表中,查询了死了的数据.
                            // 实际上就是查询了 update_time 字段已经超时的数据.
                            List<Integer> ids = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                                    .findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                            if (ids != null && ids.size() > 0) {
                                // 这个就很简单啦. 就是有超时的就直接delete掉.
                                XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                            }
                            
                            // fresh online address (admin/executor)
                            // 这里看作者注释是说要刷新在线的地址. 那么我猜可能是因为上面remove了dead数据. 所以这里可能要刷新.
                            HashMap<String, List<String>> appAddressMap = new HashMap<String, List<String>>();
                            // 我们看,这里又查询了xxl_job_registry表. 那么查询的是什么呢?? 实际上就是还未超时的数据.
                            // 那么初次使用时, 没有任何初始化数据,所以这里一定是空.
                            List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                                    .findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                            // 由于初次使用这里是空, 所以下面这个if的逻辑,我们先过掉.
                            // 我们先主要关系调用中心,启动阶段做了哪些事情.
                            // 算了也看一下吧. 里面好像也没啥复杂的逻辑.
                            if (list != null) {
                                for (XxlJobRegistry item : list) {
                                    // 这里就是遍历了一下尚未超市的jobRegistry. 判断jobRegistry的. RegistryGroup是否是执行器.
                                    // 如果是执行器的话,那么就执行下面的if逻辑嘛.
                                    if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
                                        // 获取了xxl_job_registry表中registry_key字段的值.
                                        String appname = item.getRegistryKey();
                                        
                                        // 整个下面的方法也不复杂, 其实最终就是填充了上面的appAddressMap.
                                        // key: registry_key value: List<String> registry_value列表.
                                        List<String> registryList = appAddressMap.get(appname);
                                        if (registryList == null) {
                                            registryList = new ArrayList<String>();
                                        }
                                        
                                        if (!registryList.contains(item.getRegistryValue())) {
                                            registryList.add(item.getRegistryValue());
                                        }
                                        appAddressMap.put(appname, registryList);
                                    }
                                }
                            }
                            
                            // fresh group address
                            // 这里看作者注释是要刷新group address.
                            for (XxlJobGroup group : groupList) {
                                // 代码执行到这里, 如果xxl_job_registry表中存在尚未超时的数据,那么appAddressMap肯定不为空.
                                List<String> registryList = appAddressMap.get(group.getAppname());
                                String addressListStr = null;
                                
                                // 下面这个if逻辑其实就是把xxl_job_registry中 registry_key相同的数据的registry_value使用逗号拼接起来.
                                // 居然写了这么多.
                                if (registryList != null && !registryList.isEmpty()) {
                                    Collections.sort(registryList);
                                    StringBuilder addressListSB = new StringBuilder();
                                    for (String item : registryList) {
                                        addressListSB.append(item).append(",");
                                    }
                                    addressListStr = addressListSB.toString();
                                    addressListStr = addressListStr.substring(0, addressListStr.length() - 1);
                                }
                                // 重新设置了更新后的address_list和update_time
                                group.setAddressList(addressListStr);
                                group.setUpdateTime(new Date());
                                // 更新xxl_job_group表了.
                                XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                        }
                    }
                    
                    // 这里还有一段有意思的代码, 这里休眠了30秒.
                    try {
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
            }
        });
        registryMonitorThread.setDaemon(true);
        registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
        registryMonitorThread.start();
        
        // 看到这里了，我们总结一下这个start方法. 主要就做了两件事情：
        // 1.实例化一个线程池. 具体这个线程池做什么用, 在哪里用. 我们还不清楚.
        // 2.启动了一个线程. 这个线程可理解为每30秒, 会从xxl_job_registry表中取出registry_value(执行器地址)更新到xxl_job_group表中.
        // 那么到这里, 执行器启动过程中的源码已经看完了.
        
        // think about it.
        // 那么我们也可以想一下, xxl_job_registry表中的执行器数据是怎么来的呢?? update_time又是如何更新的呢??
        // 如果update_time不更新的话就会在启动阶段被判定为超时delete掉啊.
        // 那么会不会是执行器自己更新或者通知调度中心去更新啊. 更好的选择肯定是执行器去通知调度器更新, 可以理解为心跳机制.
    }
    
    public void toStop() {
        toStop = true;
        
        // stop registryOrRemoveThreadPool
        registryOrRemoveThreadPool.shutdownNow();
        
        // stop monitir (interrupt and wait)
        registryMonitorThread.interrupt();
        try {
            registryMonitorThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
    
    // ---------------------- helper ----------------------
    // 初步一看这个方法好像挺长的, 但也没啥, 就是让线程池执行了任务而已.
    public ReturnT<String> registry(RegistryParam registryParam) {
        
        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup()) || !StringUtils
                .hasText(registryParam.getRegistryKey()) || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        
        // async execute
        // 这里我们看到了registryOrRemoveThreadPool这个线程池, 是不是感觉有点眼熟??
        // 不就是调度中心启动阶段初始化的那个线程池么.. 就在这个类的JobRegistryHelper.start方法中. 哎,这个时候我们才发现我们就在这个类中啊.
        // 所以这个时候我们看一下这个类方法列表, 我们发现居然还有registryRemove方法. 那这个会不会是调用的那个registry remove方法呢??
        // 我们也先不管它.可以以后去看.
        // 那么这里我们看什么呢?? 我们肯定去看run方法啊.
        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 直接写库啦. 直接update xxl_job_registry 表数据的update_time字段.
                // 那这里是不是在做job_registry续命动作.
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                        .registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                                registryParam.getRegistryValue(), new Date());
                
                // 不过这里也很奇怪, 作者是先update, update不到就insert.
                if (ret < 1) {
                    XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                            .registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                                    registryParam.getRegistryValue(), new Date());
                    
                    // fresh
                    // 看这里注释还有一个刷新动作, 其实不看代码我们也应该猜到了, 刷新啥啊?? 肯定是xxl_job_group啊
                    // 调度中心在启动阶段不就干的是这个事嘛.
                    // 既然注册看完了, 我们就去看看取消注册的逻辑, 还是先从执行器开始看起吧.
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });
        
        return ReturnT.SUCCESS;
    }
    
    // 从整体上看,这个方法好像也没什么, if逻辑直接跳过.
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        
        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup()) || !StringUtils
                .hasText(registryParam.getRegistryKey()) || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        
        // async execute
        // 这个线程池知道了吧? 上面是不是已经说过JobRegistryHelper.start方法中实例化的.
        // 我们还是直接看run方法吧.
        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 哎. 一看很简单, 就直接delete from xxl_job_registry了.
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                        .registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                                registryParam.getRegistryValue());
                if (ret > 0) {
                    // fresh
                    // 又refresh. 然而我们已经知道啥也没干了.
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });
        
        return ReturnT.SUCCESS;
    }
    
    // 然而,当我们进到这个方法来的时候, 我们才发现, 原来作者啥也没干.
    // 为什么?? 为什么没做也没问题??
    // 因为别忘了, 调度中心启动的时候, 就已经有一个线程在做这个事情了. 每30秒做一次..
    private void freshGroupRegistryInfo(RegistryParam registryParam) {
        // Under consideration, prevent affecting core tables
    }
}
