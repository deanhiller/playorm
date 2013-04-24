package org.playorm.cron.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.playorm.cron.api.CronService;
import org.playorm.cron.api.CronServiceFactory;
import org.playorm.cron.api.PlayOrmCronJob;
import org.playorm.cron.bindings.CronProdBindings;

import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.FactorySingleton;

public class TestBasic {

	private CronService server1Monitor;
	private CronService server2Monitor;
	private Runnable clusterChecker1;
	private Runnable clusterChecker2;
	private NoSqlEntityManagerFactory factory;
	private MockListener listener1;
	private MockListener listener2;
	private MockHash mockHash;

	@Before
	public void setup() {
		factory = FactorySingleton.createFactoryOnce();
		
		MockScheduler mock = new MockScheduler();
		mockHash = new MockHash();
		
		listener1 = new MockListener();
		listener2 = new MockListener();
		
		int rate = 5*60*1000;
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(CronServiceFactory.NOSQL_MGR_FACTORY, factory);
		props.put(CronProdBindings.SCHEDULER, mock);
		props.put(CronProdBindings.HASH_GENERATOR, mockHash);
		props.put(CronServiceFactory.SCAN_RATE_MILLIS, ""+rate);
		props.put(CronServiceFactory.HOST_UNIQUE_NAME, "host1");
		server1Monitor = CronServiceFactory.create(props);
		props.put(CronServiceFactory.HOST_UNIQUE_NAME, "host2");
		server2Monitor = CronServiceFactory.create(props);
		server1Monitor.addListener(listener1);
		server2Monitor.addListener(listener2);
		
		server1Monitor.start();
		clusterChecker1 = mock.getLastRunnable();
		
		server2Monitor.start();
		clusterChecker2 = mock.getLastRunnable();
		
		clusterChecker1.run();
		clusterChecker2.run();
		Assert.assertNull(listener1.getLastFiredMonitor());
		Assert.assertNull(listener2.getLastFiredMonitor());
	}

	@Test
	public void testBasic() throws InterruptedException {
		PlayOrmCronJob monitor = new PlayOrmCronJob();
		monitor.setId("asdf");
		monitor.setTimePeriodMillis(1);
		monitor.addProperty("email", "dean@xsoftware");
		monitor.addProperty("myName", "dean");
		server1Monitor.saveMonitor(monitor);

		PlayOrmCronJob m = server1Monitor.getMonitor(monitor.getId());
		Assert.assertEquals(monitor.getTimePeriodMillis(), m.getTimePeriodMillis());
		String email1 = monitor.getProperties().get("email");
		String emailB = m.getProperties().get("email");
		Assert.assertEquals(email1, emailB);
		
		mockHash.addReturnValue(1); //identify the second server and run server 1 then server 2
		clusterChecker1.run();
		mockHash.addReturnValue(1);
		clusterChecker2.run();

		Assert.assertNull(listener1.getLastFiredMonitor());
		m = listener2.getLastFiredMonitor();
		Assert.assertEquals(monitor.getId(), m.getId());
		
		Thread.sleep(30);// now if we run again, the period is one millisecond so it should run again
		
		mockHash.addReturnValue(1);
		clusterChecker2.run();
		
		m = listener2.getLastFiredMonitor();
		Assert.assertEquals(monitor.getId(), m.getId());
		

		String email2 = m.getProperties().get("email");
		Assert.assertEquals(email1, email2);
	}
}
