package com.alvazan.orm.api.base.spi;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.joda.time.LocalDateTime;


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

	private static final String HOST_NAME;

	private static long lastTimeStamp;
	
	static {
		LocalDateTime time = new LocalDateTime(2012, 6, 1, 0, 0);
		long baseTime = time.toDate().getTime();
		lastTimeStamp = System.currentTimeMillis() - baseTime;
		
		try {
			HOST_NAME = createHostName();
		} catch(Throwable e) {
			throw new RuntimeException("Could not create a ip needed for unique key gen.\n" +
					"PLEASE if you are on linux configure it properly and\n" +
					" run this simple code to test(this code fails right" +
					" now returning localhost instead of the hostname!!!)\n" +
					"InetAddress local = InetAddress.getLocalHost();\n" +
					"String hostname = local.getHostName();", e);
		}
	}
	
	@Override
	public String generateNewKey(Object entity) {
		return generateKey();
	}
	
	public static synchronized String generateKey() {
		long time = lastTimeStamp++;
		return time+":"+HOST_NAME;
	}

	private static String createHostName() throws UnknownHostException {
		String address;
		InetAddress local = InetAddress.getLocalHost();
		address = local.getHostName();
		if(address.contains(".")) {
			//let's strip it down to just the raw host name since all hosts will have the same domain
			int index = address.indexOf(".");
			address = address.substring(0, index);
		}
		
		if(address.contains("localhost"))
			throw new RuntimeException("Call to InetAddress.getLocalHost().getHostname() == localhost.  This must be fixed(you are most likely on linux!!) or unique keys will not be generated");
		return address;
	}

	public static String getHostname() {
		return HOST_NAME;
	}
}
