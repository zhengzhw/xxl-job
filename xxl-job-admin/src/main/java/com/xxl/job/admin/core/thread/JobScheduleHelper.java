package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);
    
    private static JobScheduleHelper instance = new JobScheduleHelper();
    
    public static JobScheduleHelper getInstance() {
        return instance;
    }
    
    public static final long PRE_READ_MS = 5000;    // pre read
    
    private Thread scheduleThread;
    
    private Thread ringThread;
    
    private volatile boolean scheduleThreadToStop = false;
    
    private volatile boolean ringThreadToStop = false;
    
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();
    
    // 从整体上看, 这个方法启动了2个线程, 一个叫scheduleThread, 另外一个叫ringThread.
    // 现在我们也不知道这2个线程到底要干啥, 我们就看他的run方法呗.
    public void start() {
        
        // schedule thread
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // 这俩二话不说,上来就先让线程sleep, 这个sleep也挺有讲究. 5000 - System.currentTimeMillis()%1000
                    // System.currentTimeMillis()%1000 是什么? 是不是当前时间的毫秒数.
                    // 5000 - 当前时间毫秒数是什么. 是不是就代表整个线程的唤醒时间是 整秒, 毫秒数为0.
                    // 比如现在时间是 13分18秒230毫秒, 线程经过整个操作. 线程唤醒时间是不是就变成了13分23秒.
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");
                
                // pre-read count: treadpool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)
                // 这里有个预取数量的计算. 在说这个计算前, 我们先来说下xxl-job的快慢线程池. fastTriggerPool和slowTriggerPool
                // 这两个线程池是XxlJobScheduler.init时,调用JobTriggerPoolHelper.toStart方法初始化的. 核心线程数固定, 但是非核心线程可通过参数进行配置.
                // 我们查看配置
                // fast trigger pool 有 xxl.job.triggerpool.fast.max=200
                // slow trigger pool 有 xxl.job.triggerpool.slow.max=100
                // 那么按照作者的公式, 此处就是 (200+100) * 20 = 6000 所以说这个preReadCount 就是6000
                int preReadCount =
                        (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig()
                                .getTriggerPoolSlowMax()) * 20;
                
                // 接下来这个线程就剩下这个while循环了. 这个循环还挺长, 居然写了将近200行. 开源...呵呵..
                while (!scheduleThreadToStop) {
                    
                    // Scan Job
                    long start = System.currentTimeMillis();
                    
                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;
                    
                    boolean preReadSuc = true;
                    try {
                        // 这一上来有点懵啊. 这是在干啥?? jdbc?
                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        connAutoCommit = conn.getAutoCommit();
                        conn.setAutoCommit(false);
                        
                        // 看看这是又在干嘛? 我们看这个sql. for update... 这是要给数据库表加锁啊.
                        // 为什么这么干有人知道么?? 我猜测可能是因为调用中心支持高可用部署, 不能在任何时间存在多个实例一起调度任务吧.
                        // 是不是分布式锁?? 其实原理是一样的, 只不过是使用数据库做的.
                        preparedStatement = conn.prepareStatement(
                                "select * from xxl_job_lock where lock_name = 'schedule_lock' for update");
                        preparedStatement.execute();
                        
                        // tx start
                        
                        // 1、pre read
                        // 看作者注释,这里是要执行预读了. 怎么预读呢??
                        // 从xxl_job_info表中读出下次执行时间在5秒内的数据. 为什么是5秒内?? PRE_READ_MS = 5000ms.
                        long nowTime = System.currentTimeMillis();
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao()
                                .scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        
                        // 如果5秒内有待执行任务, 那么我们看看作者干啥了??
                        if (scheduleList != null && scheduleList.size() > 0) {
                            // 2、push time-ring
                            // 整个for循环里面全是分支逻辑, 我们还是得看一看整个push time-ring 是什么东西.
                            // 其实一会看了下面的代码就知道了, 整个time-ring 就是一个时间轮. 采用Map<Integer,List>实现的.
                            for (XxlJobInfo jobInfo : scheduleList) {
                                // time-ring jump
                                // 这里这个判断, 当前时间 > 任务下次执行时间+5秒.. 说明什么?? 说明任务都已经超时5秒了还没执行啊.
                                // 说白了是要执行超时的任务. 超时超过5秒的任务.
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                                    logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());
                                    
                                    // 1、misfire match
                                    // 这里是一个job的调度超时策略, 默认是什么也不做.
                                    MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum
                                            .match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                    
                                    // 如果是FIRE_ONCE_NOW. 那么直接trigger.
                                    if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
                                        // FIRE_ONCE_NOW 》 trigger
                                        // 调度过期补偿.
                                        JobTriggerPoolHelper
                                                .trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null,
                                                        null);
                                        logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo
                                                .getId());
                                    }
                                    
                                    // 2、fresh next
                                    // 这里还有一个refresh next 是干什么的?? 顺便看一看吧. 我估计无非就是要更新任务的下一次执行时间.
                                    refreshNextValidTime(jobInfo, new Date());
                                    
                                    // 居然也是处理超时任务的, 这里是处理超时时间<5秒的.
                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                                    
                                    // 1、trigger
                                    // 你看这里的也是trigger. 是不是和上面超时时间超过5秒的差不多? 但是这里的TriggerType不一样.
                                    // 这里是CRON触发. 上面的是调度过期补偿.
                                    JobTriggerPoolHelper
                                            .trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                    logger.debug(
                                            ">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());
                                    
                                    // 2、fresh next
                                    refreshNextValidTime(jobInfo, new Date());
                                    
                                    // next-trigger-time in 5s, pre-read again
                                    // 这里又看了一下当前这个任务的下次执行时间, 如果说这个任务的下次执行时间还是在5秒内.
                                    // 那么就加入 timeRing 时间轮.
                                    if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo
                                            .getTriggerNextTime()) {
                                        
                                        // 1、make ring second
                                        // 这个算法很好理解啊, 就是拿任务的下次执行时间换算成秒数,然后对60取余.
                                        // 其实啊, xxl-job的这个时间轮. 就是一个map, key 是0-59 value 就是对应秒要执行的任务列表.
                                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                        
                                        // 2、push time ring
                                        pushTimeRing(ringSecond, jobInfo.getId());
                                        
                                        // 3、fresh next
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                    }
                                } else {
                                    // 好了, 到这里就是正常未超时, 在未来5秒内要执行的任务了.
                                    // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time
                                    
                                    // 1、make ring second
                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                    
                                    // 2、push time ring
                                    pushTimeRing(ringSecond, jobInfo.getId());
                                    
                                    // 3、fresh next
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                }
                                
                            }
                            
                            // 3、update trigger info
                            // 遍历更新了jobInfo的最后执行时间, 下次执行时间, 调度状态
                            for (XxlJobInfo jobInfo : scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }
                        } else {
                            preReadSuc = false;
                        }
                        
                        // tx stop
                        
                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {
                        
                        // commit
                        // 这一大坨都是处理数据库链接和事务的. 我们不看.
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                        
                        // close PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                    // 我们看一下这里, 这里有做什么比较重要的东西嘛?
                    long cost = System.currentTimeMillis() - start;
                    
                    // Wait seconds, align second
                    // 又在sleep.  但是我们发现一个问题啊, 我们好像只看到作者往timeRing放任务了,没看着怎么取的啊.
                    // 要不然调度少一部分啊. 肯定有啊. 就在下面.
                    if (cost < 1000) {  // scan-overtime, not wait
                        try {
                            // pre-read period: success > scan each second; fail > skip this period;
                            TimeUnit.MILLISECONDS
                                    .sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }
                    
                }
                
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();
        
        // ring thread
        // 又一个线程. 这代码都写一大坨,蛋疼.
        // 但是我们为了了解xxl-job, 我们还是要硬着头皮继续看.. 所以说代码质量的重要性啊. 继续run方法.
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!ringThreadToStop) {
                    // align second
                    // 实际上是对齐时间啊, 为什么对齐呢? 想想它的时间论, 是不是按照秒做key的.
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                    } catch (InterruptedException e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    
                    try {
                        // second data
                        List<Integer> ringItemData = new ArrayList<>();
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);   // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
                        // 看下面的这个for循环. 一看这里面加减取余的. 就很懵.
                        // 我们看到作者到底想干啥.
                        // 假设当前时间是第5秒, 那么(5+60-0)%60=5; (5+60-1)%60=4; 那么也就是向前多读出一秒的任务嘛.
                        for (int i = 0; i < 2; i++) {
                            List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }
                        }
                        
                        // ring trigger
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays
                                .asList(ringItemData));
                        if (ringItemData.size() > 0) {
                            // do trigger
                            for (int jobId : ringItemData) {
                                // do trigger
                                // 遍历 调用. 这里我们就不去看了. 不就是去调用执行器嘛.
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }
    
    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        // 你看, 就是设置了 jobInfo最后执行时间和洗一次执行时间.
        Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            logger.warn(
                    ">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
        }
    }
    
    private void pushTimeRing(int ringSecond, int jobId) {
        // push async ring
        List<Integer> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Integer>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);
        
        logger.debug(
                ">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData));
    }
    
    public void toStop() {
        
        // 1、stop schedule
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        
        // if has ring data
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData != null && tmpData.size() > 0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        
        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        
        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }
    
    
    // ---------------------- tools ----------------------
    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (ScheduleTypeEnum.FIX_RATE
                == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
        }
        return null;
    }
    
}
