package org.playorm.cron.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.playorm.cron.api.CronService;
import org.playorm.cron.api.CronServiceFactory;
import org.playorm.cron.api.PlayOrmCronJob;
import org.playorm.cron.bindings.CronProdBindings;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.FactorySingleton;

public class TestEpochOffset {

	private CronService server1Monitor;
	private Runnable clusterChecker1;
	private NoSqlEntityManagerFactory factory;
	private MockListener listener1;
	private MockHash mockHash;
	private MockTime mockTime;

	@Before
	public void setup() {
		factory = FactorySingleton.createFactoryOnce();
		MockScheduler mock = new MockScheduler();
		mockHash = new MockHash();
		listener1 = new MockListener();
		mockTime = new MockTime();
		
		int rate = 1000;
		
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(CronServiceFactory.NOSQL_MGR_FACTORY, factory);
		props.put(CronProdBindings.SCHEDULER, mock);
		props.put(CronProdBindings.HASH_GENERATOR, mockHash);
		props.put(CronProdBindings.CURRENT_TIME, mockTime);
		props.put(CronServiceFactory.SCAN_RATE_MILLIS, ""+rate);
		props.put(CronServiceFactory.HOST_UNIQUE_NAME, "host1");
		server1Monitor = CronServiceFactory.create(props);
		server1Monitor.addListener(listener1);
		
		server1Monitor.start();
		clusterChecker1 = mock.getLastRunnable();
		
		mockHash.addReturnValue(0); //identify the first server and run server 1
		mockTime.addReturnTime(15000);
		clusterChecker1.run();
		Assert.assertNull(listener1.getLastFiredMonitor());
	}

	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}
	
	@Test
	public void testOffsetFromEpoch() throws InterruptedException {
		PlayOrmCronJob monitor = new PlayOrmCronJob();
		monitor.setId("coolMon");
		monitor.setEpochOffset(3000L);
		monitor.setTimePeriodMillis(10000);
		monitor.addProperty("email", "dean@xsoftware");
		monitor.addProperty("myName", "dean");
		server1Monitor.saveMonitor(monitor);

		PlayOrmCronJob m = server1Monitor.getMonitor(monitor.getId());
		Assert.assertEquals(monitor.getTimePeriodMillis(), m.getTimePeriodMillis());
		String email1 = monitor.getProperties().get("email");
		String emailB = m.getProperties().get("email");
		Assert.assertEquals(email1, emailB);

		mockTime.addReturnTime(32000);
		mockHash.addReturnValue(0); //identify the first server and run server 1
		clusterChecker1.run();
		Assert.assertNull(listener1.getLastFiredMonitor());

		mockTime.addReturnTime(32600);
		mockHash.addReturnValue(0); //identify the first server and run server 1
		clusterChecker1.run();		

		PlayOrmCronJob job = listener1.getLastFiredMonitor();
		Assert.assertEquals(monitor.getId(), job.getId());
		
		mockTime.addReturnTime(39000);
		mockHash.addReturnValue(0); //identify the first server and run server 1
		clusterChecker1.run();
		Assert.assertNull(listener1.getLastFiredMonitor());		
		
		mockTime.addReturnTime(50000);
		mockHash.addReturnValue(0); //identify the first server and run server 1
		clusterChecker1.run();
		
		PlayOrmCronJob job2 = listener1.getLastFiredMonitor();
		Assert.assertEquals(monitor.getId(), job2.getId());
	}
}
