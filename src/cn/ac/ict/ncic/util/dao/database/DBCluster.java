/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ict.ncic.util.dao.database;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mwm
 */
public class DBCluster {

    protected String name;// defined by user
    protected List<DBCluster> dbClusters = new ArrayList<DBCluster>();
    public static final int SINGLE_DB_NODE = 0;
    public static final int RAC = 1;

    protected DBCluster(String _name) {
        this.name = _name;
    }

    public String getName() {
        return name;
    }

    protected void addDBCluster(DBCluster _dbCluster) {
        this.dbClusters.add(_dbCluster);
    }

    protected List<DBCluster> getDBClusters() {
        return dbClusters;
    }
}
