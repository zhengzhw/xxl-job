package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by xuxueli on 17/3/1.
 */
public class ExecutorBizImpl implements ExecutorBiz {
    
    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);
    
    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }
    
    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        
        // isRunningOrHasQueue
        boolean isRunningOrHasQueue = false;
        JobThread jobThread = XxlJobExecutor.loadJobThread(idleBeatParam.getJobId());
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrHasQueue = true;
        }
        
        if (isRunningOrHasQueue) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }
        return ReturnT.SUCCESS;
    }
    
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // load old：jobHandler + jobThread
        // 加载jobThread. 从哪里加载?? 其实不看这个loadJobThread我们也能猜的差不多. 肯定是jobThreadRepository.
        // 这个jobThreadRepository是什么时候写入的数据呢?? 其实是在执行任务的时候?? 作者是为了做缓存的.
        // 那么我们第一次这个jobThread 一定是空.
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        IJobHandler jobHandler = jobThread != null ? jobThread.getHandler() : null;
        String removeOldReason = null;
        
        // valid：jobHandler + jobThread
        // 这里面有一堆的if判断, 我们好像也不是很清楚这个GlueType是怎么来的. 但是我们可以去看看调度中心那么是如何传入的.
        // 通过去看XxlJobTrigger.processTrigger方法,我们发现是从数据表xxl_job_info.glue_type字段取的.
        // 那么这个字段是什么意思?? 其实也就是我们配置任务的时候的类型. 是spring bean 还是啥. 那么我们就单看bean的吧.
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        if (GlueTypeEnum.BEAN == glueTypeEnum) {
            
            // new jobhandler
            // 这里获取了执行器在启动阶段 注册的jobHandler. 肯定是从jobThreadRepository获取了.
            // 是不是执行器在启动阶段, 读取的那些@XxlJob注解的方法??
            // 那么jobId 是什么??  是不是就是 @XxlJob("simple-test-job")里面的 simple-test-job.
            // 说到这里我们再想想我之前说的为什么注解里面的value要全局唯一?? 就是因为作者在这里使用了value 作为map的key. 不唯一行么?? 明显会被覆盖.
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());
            
            // 看了下面的代码 你就会发现, 拿出了jobThread 和 jobHandler. 啥也没干啊.
            // 我们可以继续往下看看. 毕竟总归要去调用方法的嘛.
            // valid old jobThread
            if (jobThread != null && jobHandler != newJobHandler) {
                // change handler, need kill old thread
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";
                
                jobThread = null;
                jobHandler = null;
            }
            
            // valid handler
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }
            
        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {
            
            // valid old jobThread
            if (jobThread != null && !(jobThread.getHandler() instanceof GlueJobHandler
                    && ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam
                    .getGlueUpdatetime())) {
                // change handler or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";
                
                jobThread = null;
                jobHandler = null;
            }
            
            // valid handler
            if (jobHandler == null) {
                try {
                    IJobHandler originJobHandler = GlueFactory.getInstance()
                            .loadNewInstance(triggerParam.getGlueSource());
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        } else if (glueTypeEnum != null && glueTypeEnum.isScript()) {
            
            // valid old jobThread
            if (jobThread != null && !(jobThread.getHandler() instanceof ScriptJobHandler
                    && ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam
                    .getGlueUpdatetime())) {
                // change script or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";
                
                jobThread = null;
                jobHandler = null;
            }
            
            // valid handler
            if (jobHandler == null) {
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(),
                        triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }
        
        // executor block strategy
        // 第一次 jobThread为空, 所以这里不会进.
        if (jobThread != null) {
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum
                    .match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {
                // discard when running
                if (jobThread.isRunningOrHasQueue()) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            "block strategy effect：" + ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
                }
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {
                // kill running jobThread
                if (jobThread.isRunningOrHasQueue()) {
                    removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();
                    
                    jobThread = null;
                }
            } else {
                // just queue trigger
            }
        }
        
        // replace thread (new or exists invalid)
        // 第一次jobThread为空,那么先注册jobThread.
        if (jobThread == null) {
            jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }
        
        // push data to queue
        // 看到这里总应该去调用了吧?? 我们进去看看.
        // 然而进去发现还没有, 作者又放入了一个queue中. 整个xxl-job用了大量的线程池+异步技术.
        ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
        return pushResult;
    }
    
    @Override
    public ReturnT<String> kill(KillParam killParam) {
        // kill handlerThread, and create new one
        JobThread jobThread = XxlJobExecutor.loadJobThread(killParam.getJobId());
        if (jobThread != null) {
            XxlJobExecutor.removeJobThread(killParam.getJobId(), "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }
        
        return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread already killed.");
    }
    
    @Override
    public ReturnT<LogResult> log(LogParam logParam) {
        // log filename: logPath/yyyy-MM-dd/9999.log
        String logFileName = XxlJobFileAppender
                .makeLogFileName(new Date(logParam.getLogDateTim()), logParam.getLogId());
        
        LogResult logResult = XxlJobFileAppender.readLog(logFileName, logParam.getFromLineNum());
        return new ReturnT<LogResult>(logResult);
    }
    
}
