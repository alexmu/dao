/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ict.ncic.util.dao;

import cn.ac.ict.ncic.util.dao.database.DBCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author muweimin
 */
public class DaoPool {

    private static Map<String, Dao> daoPoolMap = new HashMap<String, Dao>();
    private static List<String> daoPoolNames = new ArrayList<String>();

    public static void putDao(DBCluster _dbCluster) {
        daoPoolMap.put(_dbCluster.getName(), new Dao(_dbCluster));
        daoPoolNames.add(_dbCluster.getName());
    }

    public static void putDao(List<DBCluster> _dbClusters) {
        for (DBCluster dbCluster : _dbClusters) {
            putDao(dbCluster);
        }
    }

    public static Dao getDao() {
        return getSomeDao();
    }

    public static Dao getDao(String _clusterName) {
        return getAppointedDao(_clusterName);
    }

    public static Dao getAppointedDao(String _clusterName) {
        return daoPoolMap.get(_clusterName);
    }

    public static Dao getSomeDao() {
        int randNum = (int) (Math.random() * daoPoolNames.size());
        return daoPoolMap.get(daoPoolNames.get(randNum));
    }

    public static Dao getSomeAvailableDao() throws Exception {
        
        //start from a random sequence to avoid all client node try to connect the same dbcluster at the same time
        int startIndex = (int) (Math.random() * daoPoolNames.size());

        for (int i = startIndex; i < daoPoolNames.size(); i++) {
            Dao dao = daoPoolMap.get(daoPoolNames.get(i));
            if (dao.isAvailable()) {
                return dao;
            }
        }
        throw new Exception("no available database cluster,please check");
    }
}
