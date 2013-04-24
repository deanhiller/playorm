package org.playorm.cron.bindings;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.playorm.cron.api.CronServiceFactory;
import org.playorm.cron.impl.Config;
import org.playorm.cron.impl.CurrentTime;
import org.playorm.cron.impl.CurrentTimeImpl;
import org.playorm.cron.impl.HashGenerator;
import org.playorm.cron.impl.HashGeneratorImpl;

import com.google.inject.Binder;
import com.google.inject.Module;

public class CronProdBindings implements Module {

	public static final String SCHEDULER = "org.playorm.monitor.scheduler";
	public static final String HASH_GENERATOR = "org.playorm.monitor.hashGenerator";
	public static final String CURRENT_TIME = "org.playorm.monitor.currentTime";
	
	private ScheduledExecutorService svc;
	private long rate;
	private Config config;
	private HashGenerator generator;
	private CurrentTime time;
	
	public CronProdBindings(Map<String, Object> properties) {
		Object obj = properties.get(SCHEDULER);
		if(obj == null)
			svc = Executors.newScheduledThreadPool(1);
		else
			svc = (ScheduledExecutorService) obj;
		
		Object ttt = properties.get(CURRENT_TIME);
		if(ttt == null)
			time = new CurrentTimeImpl();
		else
			time = (CurrentTime) ttt;
		
		Object rateObj = properties.get(CronServiceFactory.SCAN_RATE_MILLIS);
		if(!(rateObj instanceof String))
			throw new IllegalArgumentException("SCAN_RATE_MILLIS must be a long as a String");
		String rateStr = (String) rateObj;
		rate = Long.parseLong(rateStr);
		
		String host = (String) properties.get(CronServiceFactory.HOST_UNIQUE_NAME);
		
		config = new Config(rate, host);
		
		generator = (HashGenerator)properties.get(HASH_GENERATOR);
		if(generator == null)
			generator = new HashGeneratorImpl();
	}

	@Override
	public void configure(Binder binder) {
		binder.bind(ScheduledExecutorService.class).toInstance(svc);
		binder.bind(HashGenerator.class).toInstance(generator);
		
		binder.bind(Config.class).toInstance(config );
		binder.bind(CurrentTime.class).toInstance(time);
	}
}
