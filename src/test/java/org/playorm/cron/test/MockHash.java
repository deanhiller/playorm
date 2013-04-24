package org.playorm.cron.test;

import org.playorm.cron.impl.HashGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockHash implements HashGenerator {

	private static final Logger log = LoggerFactory.getLogger(MockHash.class);
	private Integer nodeNum;

	public void addReturnValue(Integer nodeNum) {
		log.info("add return value="+nodeNum);
		this.nodeNum = nodeNum;
	}

	@Override
	public int generate(int monitorHash, int numUpWebNodes) {
		log.info("generate number="+nodeNum);
		if(nodeNum == null)
			throw new IllegalArgumentException("you need to call addReturnValue before this is called in the test");
		Integer temp = nodeNum;
		nodeNum = null;
		return temp;
	}

}
