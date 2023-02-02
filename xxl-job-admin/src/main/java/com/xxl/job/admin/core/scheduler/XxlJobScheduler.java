package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler {
    
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);
    
    /**
     * 这个init方法做了很多事情, 我们初步了解一下.
     *
     * 1.我们优先关注JobRegistryHelper.getInstance().start();这段代码.
     *
     * @throws Exception
     */
    public void init() throws Exception {
        // init i18n 初始化国际化相关内容,我们看源码可以先不看这里.
        initI18n();
        
        // admin trigger pool start
        // 初始化了xxl-job的快慢线程池.
        JobTriggerPoolHelper.toStart();
        
        // admin registry monitor run
        // 看到getInstance,那么一定使用了单例. 调用了start方法.
        JobRegistryHelper.getInstance().start();
        
        // admin fail-monitor run
        JobFailMonitorHelper.getInstance().start();
        
        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        JobCompleteHelper.getInstance().start();
        
        // admin log report start
        JobLogReportHelper.getInstance().start();
        
        // start-schedule  ( depend on JobTriggerPoolHelper )
        // 这里就是xxl-job调度的核心代码了. 如何实现任务调度, 高可用下如何保证同一时刻下只有一个调度中心执行任务调度.
        // 我们进去看一看这个start就知道了.
        JobScheduleHelper.getInstance().start();
        
        logger.info(">>>>>>>>> init xxl-job admin success.");
    }
    
    
    public void destroy() throws Exception {
        
        // stop-schedule
        JobScheduleHelper.getInstance().toStop();
        
        // admin log report stop
        JobLogReportHelper.getInstance().toStop();
        
        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();
        
        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();
        
        // admin registry stop
        JobRegistryHelper.getInstance().toStop();
        
        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();
        
    }
    
    // ---------------------- I18n ----------------------
    
    private void initI18n() {
        for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }
    
    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address == null || address.trim().length() == 0) {
            return null;
        }
        
        // load-cache
        // 从jvm中取. 那么第一次肯定取不到啊. 为什么说第一次取不到呢?? 没有作者是在取完后放入缓存的.
        // 主要是解决第二次取的问题.
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }
        
        // set-cache
        // 我们看到了啊. 这里是返回了ExecutorBizClient实现. 那么我们一会就看这个实现呗.
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());
        
        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }
    
}
