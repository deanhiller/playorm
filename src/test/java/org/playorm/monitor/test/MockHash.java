package org.playorm.monitor.test;

import org.playorm.monitor.impl.HashGenerator;

public class MockHash implements HashGenerator {

	private Integer nodeNum;

	public void addReturnValue(Integer nodeNum) {
		this.nodeNum = nodeNum;
	}

	@Override
	public int generate(int monitorHash, int numUpWebNodes) {
		if(nodeNum == null)
			throw new IllegalArgumentException("you need to call addReturnValue before this is called in the test");
		Integer temp = nodeNum;
		nodeNum = null;
		return temp;
	}

}
