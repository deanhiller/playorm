package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.util.List;
import java.util.concurrent.Future;

import com.datastax.driver.core.ResultSet;

public interface StartQueryListener {

    List<Future<ResultSet>> start();

}
