package com.alvazan.orm.api.base.spi;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Some programmers I have met think random == unique.  It does NOT.  Generate 11 random
 * numbers between 1 and 10 and the 11th "randomly generated number will match one of the
 * previous 10 meaning it is NOT unique.  That said, we could have used
 * http://johannburkard.de/blog/programming/java/Java-UUID-generators-compared.html
 * to generate UUID's here but instead we wanted smaller keys so we use the following method
 * for generation which WILL be unique within a cluster of machines with unique ips.  The
 * algorithm is VERY simple host-specialtimestamp where special timestamp is time in 
 * millis BUT in the case where two people called this method on the same machine, the last
 * timestamp given was stored so we increment by one such that there will never be 
 * a non-unique key.  
 */
public class UniqueKeyGenerator implements KeyGenerator {

	private static final Logger log = LoggerFactory.getLogger(UniqueKeyGenerator.class);
	private static String ipAddress;
	private static long lastTimeStamp = 0;
	
	static {
		try {
			createHostName();
		} catch(Throwable e) {
			log.warn("Could not create a ip needed for unique key gen.\n" +
					"PLEASE if you are on linux configure it properly and\n" +
					" run this simple code to test(this code fails right" +
					" now returning localhost instead of the hostname!!!)\n" +
					"InetAddress local = InetAddress.getLocalHost();\n" +
					"String hostname = local.getHostName();", e);
			
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String generateNewKey(Object entity) {
		return generateKey();
	}
	
	private static synchronized String generateKey() {
		long currentTime = System.currentTimeMillis();
		if(currentTime <= lastTimeStamp)
			currentTime = lastTimeStamp+1;
		lastTimeStamp = currentTime;
		return ipAddress+"-"+lastTimeStamp;
	}

	private static void createHostName() throws UnknownHostException {
		InetAddress local = InetAddress.getLocalHost();
		ipAddress = local.getHostName();
		if(ipAddress.contains("localhost"))
			throw new RuntimeException("Call to InetAddress.getLocalHost().getHostname() == localhost.  This must be fixed(you are most likely on linux!!) or unique keys will not be generated");
	}

}
