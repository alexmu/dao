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
public class RAC extends DBCluster {

    private String serviceName;
    private String driver;
    private String user;
    private String passwd;
    private int maxConnNum;

    public RAC(String _name, int _maxConnNum, String _serviceName, String _driver, String _user, String _passwd) {
        super(_name);
        this.serviceName = _serviceName;
        this.driver = _driver;
        this.user = _user;
        this.passwd = _passwd;        
        this.maxConnNum = _maxConnNum;
    }

    public String getUrl() {

        int dbNodeNum = dbClusters.size();
        String url = "jdbc:oracle:thin:@(DESCRIPTION="
                + "(ADDRESS_LIST="
                + "ADDRESSLISTREP)"
                + "(FAILOVER=yes)(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=SERVICENAMEREP)))";
        String addLstItem = "(ADDRESS=(PROTOCOL=TCP)(HOST=HOSTREP)(PORT=PORTREP))";
        String addLst = "";
        for (int i = 0; i < dbNodeNum; i++) {
            SingleDBNode singleDBNode = (SingleDBNode) dbClusters.get(i);
            addLst += addLstItem.replace("HOSTREP", singleDBNode.getIp()).replace("PORTREP", singleDBNode.getSidPort());
        }
        url = url.replace("ADDRESSLISTREP", addLst);
        url = url.replace("SERVICENAMEREP", serviceName);
        return url;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getDriver() {
        return driver;
    }

    public String getUser() {
        return user;
    }

    public String getPasswd() {
        return passwd;
    }

    public void addDBNode(SingleDBNode _singleDBNode) {
        super.addDBCluster(_singleDBNode);
    }

    public List<DBCluster> getDBNodes() {
        return super.getDBClusters();
    }

    public int getMaxConnNum() {
        return maxConnNum;
    }
    
}
