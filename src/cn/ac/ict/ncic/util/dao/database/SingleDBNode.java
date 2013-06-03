/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ict.ncic.util.dao.database;

/**
 *
 * @author mwm
 */
public class SingleDBNode extends DBCluster {

    private String ip;
    private String sidPort;
    private String sid;
    private String sidDriver;
    private String sidUser;
    private String sidPasswd;
    private String sidUrl;
    private int maxConnNum;

    public SingleDBNode(String _name, int _maxConnNum, String _ip, String _port, String _sid, String _driver, String _user, String _passwd) {
        super(_name);
        this.ip = _ip;
        this.sidPort = _port;
        this.sid = _sid;
        this.sidDriver = _driver;
        this.sidUser = _user;
        this.sidPasswd = _passwd;
        this.sidUrl = "jdbc:oracle:thin:@" + this.ip + ":" + this.sidPort + ":" + this.sid;
        this.maxConnNum = _maxConnNum;
    }

    public String getIp() {
        return ip;
    }

    public String getSid() {
        return sid;
    }

    public String getSidDriver() {
        return sidDriver;
    }

    public String getSidPasswd() {
        return sidPasswd;
    }

    public String getSidPort() {
        return sidPort;
    }

    public String getSidUrl() {
        return sidUrl;
    }

    public String getSidUser() {
        return sidUser;
    }

    public int getMaxConnNum() {
        return maxConnNum;
    }   
    
}
