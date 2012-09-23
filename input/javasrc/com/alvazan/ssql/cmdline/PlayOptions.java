package com.alvazan.ssql.cmdline;

import org.kohsuke.args4j.Option;

public class PlayOptions {

    @Option(name="-t",usage="where VAL can be inmemory or cassandra", required=true)
    private String type;

    @Option(name="-k",usage="where VAL is keyspace if using type of cassandra")
    private String keyspace;

    @Option(name="-s", usage="where VAL is Comma delimeted list of seeds with NO spaces such as host1:9160,host2:9160") 
    private String seeds;

    @Option(name="-v",usage="Turn on verbose logging")
    private boolean isVerbose;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getSeeds() {
		return seeds;
	}

	public void setSeeds(String seeds) {
		this.seeds = seeds;
	}

	public boolean isVerbose() {
		return isVerbose;
	}

	public void setVerbose(boolean isVerbose) {
		this.isVerbose = isVerbose;
	}

}
