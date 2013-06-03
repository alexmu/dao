/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ict.ncic.util.dao.util;

import cn.ac.ict.ncic.util.dao.database.DBCluster;
import cn.ac.ict.ncic.util.dao.database.RAC;
import cn.ac.ict.ncic.util.dao.database.SingleDBNode;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author mwm
 */
public class ClusterInfoOP {

    public static List<DBCluster> getDBClusters(String _propertyFilePath, String _clusterName) throws Exception {
        FileInputStream fis = null;
        try {

            fis = new FileInputStream(_propertyFilePath);
            Properties cfgPros = new Properties();
            cfgPros.load(fis);

            String clusterDef = cfgPros.getProperty(_clusterName);

            if (clusterDef == null) {
                throw new Exception("no cluster definition of " + _clusterName + " is found in " + _propertyFilePath);
            } else {
                return getDBClusters(clusterDef);
            }

        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (Exception ex) {
            }
        }
    }

    public static List<DBCluster> getDBClusters(String _clustersDef) throws Exception {

        if (_clustersDef == null || _clustersDef.isEmpty()) {
            throw new Exception("clusters definition should not be empty");
        }

        try {
            List<DBCluster> dbClusters = new ArrayList<DBCluster>();

            String[] clusterItem = _clustersDef.split("\\^");

            for (int i = 0; i < clusterItem.length; i++) {
                dbClusters.add(getDBCluster(clusterItem[i]));
            }

            return dbClusters;
        } catch (Exception ex) {
            throw new Exception("error happens when parsing clusters definition " + _clustersDef, ex.getCause());
        }
    }

    private static DBCluster getDBCluster(String _clusterDef) throws Exception {

        if (_clusterDef == null || _clusterDef.isEmpty()) {
            throw new Exception("cluster definition should not be empty");
        }

        try {
            String[] clusterItems = _clusterDef.split("\\|");
            if (clusterItems[1].equals("0")) {
                return new SingleDBNode(clusterItems[0], Integer.parseInt(clusterItems[2]), clusterItems[3], clusterItems[4], clusterItems[5], clusterItems[6], clusterItems[7], clusterItems[8]);
            } else if (clusterItems[1].equals("1")) {
                RAC rac = new RAC(clusterItems[0], Integer.parseInt(clusterItems[2]), clusterItems[3], clusterItems[4], clusterItems[5], clusterItems[6]);
                String[] dbNode = clusterItems[7].split(";");
                for (int i = 0; i < dbNode.length; i++) {
                    String[] dbNodeItems = dbNode[i].split(":");
                    rac.addDBNode(new SingleDBNode("", -1, dbNodeItems[0], dbNodeItems[1], "", "", "", ""));
                }
                return rac;
            } else {
                return null;
            }
        } catch (Exception ex) {
            throw new Exception("error happens when parsing cluster definition " + _clusterDef, ex.getCause());
        }
    }

    public static List<DBCluster> loadFromPropertyFile(String _propertyFilePath, String _clusterName) throws Exception {
        return getDBClusters(_propertyFilePath, _clusterName);
    }

    public static void main(String[] args) {
        try {
            List<DBCluster> dbclusters = getDBClusters("dbcluster|1|5|dbroker|oracle.jdbc.driver.OracleDriver|dbk_user|dbk_pwd|10.224.2.209:1521;10.224.2.210:1521;10.224.2.211:1521;10.224.2.212:1521");
            RAC rac = (RAC)dbclusters.get(0);
            System.out.println(rac.getUrl());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
