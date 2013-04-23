package org.playorm.cron.impl;

import java.util.Comparator;

import org.playorm.cron.impl.db.WebNodeDbo;

public class ServerComparator implements Comparator<WebNodeDbo> {

	@Override
	public int compare(WebNodeDbo o1, WebNodeDbo o2) {
		return o1.getWebServerName().compareTo(o2.getWebServerName());
	}

}
