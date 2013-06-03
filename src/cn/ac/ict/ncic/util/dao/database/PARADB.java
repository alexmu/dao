/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ict.ncic.util.dao.database;

import java.util.List;

/**
 *
 * @author mwm
 */
public class PARADB extends DBCluster {

    public PARADB(String _name) {
        super(_name);
    }

    public void addDB(DBCluster _dbCluster) {
        super.addDBCluster(_dbCluster);
    }

    public List<DBCluster> getDBs() {
        return super.getDBClusters();
    }
}
