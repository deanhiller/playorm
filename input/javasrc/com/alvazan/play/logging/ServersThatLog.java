package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.base.anno.NoSqlEmbedded;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;

@NoSqlEntity
public class ServersThatLog {

	public static final String THE_ONE_KEY = "servers.that.log";
			
	@NoSqlId(usegenerator=false)
	private String id;
	
	@NoSqlEmbedded
	private List<String> servers = new ArrayList<String>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getServers() {
		return servers;
	}

	public void setServers(List<String> servers) {
		this.servers = servers;
	}
}
