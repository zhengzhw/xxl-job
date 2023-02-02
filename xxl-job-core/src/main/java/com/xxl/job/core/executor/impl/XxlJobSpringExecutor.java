package com.xxl.job.core.executor.impl;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.impl.MethodJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.Map;


/**
 * xxl-job executor (for spring)
 *
 * @author xuxueli 2018-11-01 09:24:52
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobSpringExecutor.class);

    // start
    // 在这个方法中, 前面的几行先不关心, 我们要找的是作为xxl的执行器是如何去调度器注册的.
    // 我们先去看这个super.start() 方法, 因为一看到start我们就想起了线程.
    @Override
    public void afterSingletonsInstantiated() {

        // init JobHandler Repository
        /*initJobHandlerRepository(applicationContext);*/

        // init JobHandler Repository (for method)
        // 这里有一个注册初始化JobHandler的方法. 那它是干什么的呢??
        // 其实,我们在开发定时任务的时候 一般不都是在Spring的单例bean的方法上添加@XxlJob注解嘛.
        // 作者是搜集了一下,然后缓存了起来. 我们一起看看好了.
        initJobHandlerMethodRepository(applicationContext);

        // refresh GlueFactory
        GlueFactory.refreshInstance(1);

        // super start
        try {
            // 我们着重关注这个start.
            super.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // destroy
    @Override
    public void destroy() {
        super.destroy();
    }


    /*private void initJobHandlerRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }

        // init job handler action
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(JobHandler.class);

        if (serviceBeanMap != null && serviceBeanMap.size() > 0) {
            for (Object serviceBean : serviceBeanMap.values()) {
                if (serviceBean instanceof IJobHandler) {
                    String name = serviceBean.getClass().getAnnotation(JobHandler.class).value();
                    IJobHandler handler = (IJobHandler) serviceBean;
                    if (loadJobHandler(name) != null) {
                        throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
                    }
                    registJobHandler(name, handler);
                }
            }
        }
    }*/

    // 从整体上看, 这个方法很长,
    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }
        // init job handler from method
        // 这个看了spring的就都知道了, 这是一下把所有的 bean 定义拿出来了.
        // 想想为什么是拿bean定义, 而不是拿bean?? 因为程序在执行到这里的时候, 没办法保证所有的bean都已经创建完成.
        // 所以作者这里拿bean定义, 手动去获取bean.
        // 我们也看看下面这行代码, 在拿bean定义的时候是只要单例bean的. 所以原型bean是不是不支持??
        String[] beanDefinitionNames = applicationContext.getBeanNamesForType(Object.class, false, true);
        // 遍历bean定义, 自己getBean.
        for (String beanDefinitionName : beanDefinitionNames) {
            Object bean = applicationContext.getBean(beanDefinitionName);

            Map<Method, XxlJob> annotatedMethods = null;   // referred to ：org.springframework.context.event.EventListenerMethodProcessor.processBean
            try {
                // 下面这段代码就很好理解了, 不就是把所有使用了@XxlJob注解的方法都拿出来放到Map中嘛.
                // Map<Method, XxlJob> key 是method, value 是XxlJob注解信息.
                annotatedMethods = MethodIntrospector.selectMethods(bean.getClass(),
                        new MethodIntrospector.MetadataLookup<XxlJob>() {
                            @Override
                            public XxlJob inspect(Method method) {
                                return AnnotatedElementUtils.findMergedAnnotation(method, XxlJob.class);
                            }
                        });
            } catch (Throwable ex) {
                logger.error("xxl-job method-jobhandler resolve error for bean[" + beanDefinitionName + "].", ex);
            }
            if (annotatedMethods==null || annotatedMethods.isEmpty()) {
                continue;
            }

            // 上面拿完XxlJob, 这里就开始遍历了.实际上下面的方法也很简单,我们一起看一下好了.
            // 遍历map.
            for (Map.Entry<Method, XxlJob> methodXxlJobEntry : annotatedMethods.entrySet()) {
                // 取出方法和XxlJob注解, 实际上就是key value.
                Method executeMethod = methodXxlJobEntry.getKey();
                XxlJob xxlJob = methodXxlJobEntry.getValue();
                if (xxlJob == null) {
                    continue;
                }

                // 这个name是什么?? 不是就@XxlJob("") 括号里面的这个内容么.
                // 现在看源码是不是知道了, 这个@XxlJob注解, 是不是一定要写value.
                // 否则异常了呀.
                String name = xxlJob.value();
                if (name.trim().length() == 0) {
                    throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + bean.getClass() + "#" + executeMethod.getName() + "] .");
                }
                
                // 看这里, 这里有一个loadJobHandler, 很有意思. 实际上就是从一个map里面拿数据.
                // 作者居然还不允许name重复, 而且是全局不能重复. 你说尴尬不尬尴.
                // 即使有这样的限制为什么不是类内不重复呢..还是说有什么特别的东西.
                if (loadJobHandler(name) != null) {
                    throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
                }

                // execute method
                /*if (!(method.getParameterTypes().length == 1 && method.getParameterTypes()[0].isAssignableFrom(String.class))) {
                    throw new RuntimeException("xxl-job method-jobhandler param-classtype invalid, for[" + bean.getClass() + "#" + method.getName() + "] , " +
                            "The correct method format like \" public ReturnT<String> execute(String param) \" .");
                }
                if (!method.getReturnType().isAssignableFrom(ReturnT.class)) {
                    throw new RuntimeException("xxl-job method-jobhandler return-classtype invalid, for[" + bean.getClass() + "#" + method.getName() + "] , " +
                            "The correct method format like \" public ReturnT<String> execute(String param) \" .");
                }*/

                executeMethod.setAccessible(true);

                // init and destory
                // 这两个是什么东西?? 是不是@XxlJob注解中的 init 和 destroy啊.
                Method initMethod = null;
                Method destroyMethod = null;

                // 你看作者这里直接判断 @XxlJob注解中有没有init, 有的话就直接根据方法名反射方法了.
                if (xxlJob.init().trim().length() > 0) {
                    try {
                        initMethod = bean.getClass().getDeclaredMethod(xxlJob.init());
                        initMethod.setAccessible(true);
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException("xxl-job method-jobhandler initMethod invalid, for[" + bean.getClass() + "#" + executeMethod.getName() + "] .");
                    }
                }
                // destroy 同init 逻辑一样的, 都是直接通过方法名反射.
                if (xxlJob.destroy().trim().length() > 0) {
                    try {
                        destroyMethod = bean.getClass().getDeclaredMethod(xxlJob.destroy());
                        destroyMethod.setAccessible(true);
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException("xxl-job method-jobhandler destroyMethod invalid, for[" + bean.getClass() + "#" + executeMethod.getName() + "] .");
                    }
                }

                // registry jobhandler
                // 最后,注册JobHandler. 实际上就是添加到map中.我们可以进去看一下.
                // 上面说的这个有意思的loadJobHandler方法也是从这个map里面取值的.
                registJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));
                
                // 看到这里我们也就看完了, 我们发现就是把所有的JobHandler都添加的 一个jobHandlerRepository map中了.
            }
        }

    }

    // ---------------------- applicationContext ----------------------
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

}
