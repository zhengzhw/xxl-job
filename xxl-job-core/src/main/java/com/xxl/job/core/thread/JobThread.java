package com.xxl.job.core.thread;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


/**
 * handler thread
 *
 * @author xuxueli 2016-1-16 19:52:47
 */
public class JobThread extends Thread {
    
    private static Logger logger = LoggerFactory.getLogger(JobThread.class);
    
    private int jobId;
    
    private IJobHandler handler;
    
    private LinkedBlockingQueue<TriggerParam> triggerQueue;
    
    private Set<Long> triggerLogIdSet;        // avoid repeat trigger for the same TRIGGER_LOG_ID
    
    private volatile boolean toStop = false;
    
    private String stopReason;
    
    private boolean running = false;    // if running job
    
    private int idleTimes = 0;            // idel times
    
    
    public JobThread(int jobId, IJobHandler handler) {
        this.jobId = jobId;
        this.handler = handler;
        this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
        this.triggerLogIdSet = Collections.synchronizedSet(new HashSet<Long>());
    }
    
    public IJobHandler getHandler() {
        return handler;
    }
    
    /**
     * new trigger to queue
     *
     * @param triggerParam
     * @return
     */
    public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
        // avoid repeat
        // 防止任务重复.
        if (triggerLogIdSet.contains(triggerParam.getLogId())) {
            logger.info(">>>>>>>>>>> repeate trigger job, logId:{}", triggerParam.getLogId());
            return new ReturnT<String>(ReturnT.FAIL_CODE, "repeate trigger job, logId:" + triggerParam.getLogId());
        }
        
        // 添加到triggerQueue中. 到这里你就会发现整个调度中心通知执行器执行任务的逻辑结束了.
        // 执行器收到通知后, 只是将相关信息放入了队列, 并没有真正执行. 那么这个triggerQueue总归要又人消费吧. 我们找找.
        
        // 我们发现整个jobThread extends Thread. 那么它就一定要重写run方法. 那么执行器是什么时候执行了run方法呢??
        // 实际上是在注册jobThread的时候实例化出JobThread对象后,紧接着就去start了.
        triggerLogIdSet.add(triggerParam.getLogId());
        triggerQueue.add(triggerParam);
        return ReturnT.SUCCESS;
    }
    
    /**
     * kill job thread
     *
     * @param stopReason
     */
    public void toStop(String stopReason) {
        /**
         * Thread.interrupt只支持终止线程的阻塞状态(wait、join、sleep)，
         * 在阻塞出抛出InterruptedException异常,但是并不会终止运行的线程本身；
         * 所以需要注意，此处彻底销毁本线程，需要通过共享变量方式；
         */
        this.toStop = true;
        this.stopReason = stopReason;
    }
    
    /**
     * is running job
     *
     * @return
     */
    public boolean isRunningOrHasQueue() {
        return running || triggerQueue.size() > 0;
    }
    
    // 这个run方法也贼长.
    @Override
    public void run() {
        
        // init
        try {
            // 这个是什么?? 是不是就是@XxlJob(value = "", init="", destroy = "")的init.
            handler.init();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        
        // execute
	    // 这个方法是真的长.
        while (!toStop) {
            running = false;
            idleTimes++;
            
            TriggerParam triggerParam = null;
            try {
                // to check toStop signal, we need cycle, so wo cannot use queue.take(), instand of poll(timeout)
                // 从triggerQueue中取出调用参数.
                triggerParam = triggerQueue.poll(3L, TimeUnit.SECONDS);
                if (triggerParam != null) {
                    running = true;
                    idleTimes = 0;
                    triggerLogIdSet.remove(triggerParam.getLogId());
                    
                    // log filename, like "logPath/yyyy-MM-dd/9999.log"
                    String logFileName = XxlJobFileAppender
                            .makeLogFileName(new Date(triggerParam.getLogDateTime()), triggerParam.getLogId());
                    XxlJobContext xxlJobContext = new XxlJobContext(triggerParam.getJobId(),
                            triggerParam.getExecutorParams(), logFileName, triggerParam.getBroadcastIndex(),
                            triggerParam.getBroadcastTotal());
                    
                    // init job context
                    XxlJobContext.setXxlJobContext(xxlJobContext);
                    
                    // execute
                    XxlJobHelper.log("<br>----------- xxl-job job execute start -----------<br>----------- Param:"
                            + xxlJobContext.getJobParam());
                    
                    // 判断任务执行的超时时长设置, 如果设置了超时时长，并且大于0
                    if (triggerParam.getExecutorTimeout() > 0) {
                        // limit timeout
                        Thread futureThread = null;
                        try {
                            FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    
                                    // init job context
                                    XxlJobContext.setXxlJobContext(xxlJobContext);
                                    
                                    handler.execute();
                                    return true;
                                }
                            });
                            futureThread = new Thread(futureTask);
                            futureThread.start();
                            
                            Boolean tempResult = futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            
                            XxlJobHelper.log("<br>----------- xxl-job job execute timeout");
                            XxlJobHelper.log(e);
                            
                            // handle result
                            XxlJobHelper.handleTimeout("job execute timeout ");
                        } finally {
                            futureThread.interrupt();
                        }
                    } else {
                        // just execute
                        handler.execute();
                    }
                    
                    // valid execute handle data
                    if (XxlJobContext.getXxlJobContext().getHandleCode() <= 0) {
                        XxlJobHelper.handleFail("job handle result lost.");
                    } else {
                        String tempHandleMsg = XxlJobContext.getXxlJobContext().getHandleMsg();
                        tempHandleMsg = (tempHandleMsg != null && tempHandleMsg.length() > 50000) ? tempHandleMsg
                                .substring(0, 50000).concat("...") : tempHandleMsg;
                        XxlJobContext.getXxlJobContext().setHandleMsg(tempHandleMsg);
                    }
                    XxlJobHelper
                            .log("<br>----------- xxl-job job execute end(finish) -----------<br>----------- Result: handleCode="
                                    + XxlJobContext.getXxlJobContext().getHandleCode() + ", handleMsg = "
                                    + XxlJobContext.getXxlJobContext().getHandleMsg());
                    
                } else {
                    if (idleTimes > 30) {
                        if (triggerQueue.size() == 0) {    // avoid concurrent trigger causes jobId-lost
                            XxlJobExecutor.removeJobThread(jobId, "excutor idel times over limit.");
                        }
                    }
                }
            } catch (Throwable e) {
                if (toStop) {
                    XxlJobHelper.log("<br>----------- JobThread toStop, stopReason:" + stopReason);
                }
                
                // handle result
                StringWriter stringWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stringWriter));
                String errorMsg = stringWriter.toString();
                
                XxlJobHelper.handleFail(errorMsg);
                
                XxlJobHelper.log("<br>----------- JobThread Exception:" + errorMsg
                        + "<br>----------- xxl-job job execute end(error) -----------");
            } finally {
                if (triggerParam != null) {
                    // callback handler info
                    if (!toStop) {
                        // commonm
                        TriggerCallbackThread.pushCallBack(
                                new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTime(),
                                        XxlJobContext.getXxlJobContext().getHandleCode(),
                                        XxlJobContext.getXxlJobContext().getHandleMsg()));
                    } else {
                        // is killed
                        TriggerCallbackThread.pushCallBack(
                                new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTime(),
                                        XxlJobContext.HANDLE_COCE_FAIL, stopReason + " [job running, killed]"));
                    }
                }
            }
        }
        
        // callback trigger request in queue
	    
        while (triggerQueue != null && triggerQueue.size() > 0) {
            TriggerParam triggerParam = triggerQueue.poll();
            if (triggerParam != null) {
                // is killed
                TriggerCallbackThread.pushCallBack(
                        new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTime(),
                                XxlJobContext.HANDLE_COCE_FAIL,
                                stopReason + " [job not executed, in the job queue, killed.]"));
            }
        }
        
        // destroy
        try {
            handler.destroy();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        
        logger.info(">>>>>>>>>>> xxl-job JobThread stoped, hashCode:{}", Thread.currentThread());
    }
}
