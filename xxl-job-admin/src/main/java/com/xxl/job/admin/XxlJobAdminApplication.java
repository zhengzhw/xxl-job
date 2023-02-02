package com.xxl.job.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author xuxueli 2018-10-28 00:38:13
 */
@SpringBootApplication
public class XxlJobAdminApplication {

	// xxl-job-admin 在启动时, 有一个比较重要的配置.
	// com.xxl.job.admin.core.conf.XxlJobAdminConfig 这其实并不是一个配置类, 而是一个spring bean
	// 直接去看这个类就好.
	public static void main(String[] args) {
        SpringApplication.run(XxlJobAdminApplication.class, args);
	}

}