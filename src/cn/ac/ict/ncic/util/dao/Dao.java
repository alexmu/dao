/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ict.ncic.util.dao;

import cn.ac.ict.ncic.util.dao.database.DBCluster;
import cn.ac.ict.ncic.util.dao.database.PARADB;
import cn.ac.ict.ncic.util.dao.database.RAC;
import cn.ac.ict.ncic.util.dao.database.SingleDBNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author muweimin
 */
public class Dao {

    private ComboPooledDataSource[] cpds = null;
    private DBCluster dbCluster;
    private static Logger logger = Logger.getLogger(Dao.class.getName());

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    public boolean isAvailable() {
        ResultSet rs = null;
        boolean succeedFlag = false;
        try {
            rs = executeQuery("select * from dual");
            succeedFlag = true;
        } catch (Exception ex) {
            succeedFlag = false;
        } finally {
            Connection conn = null;
            try {
                conn = rs.getStatement().getConnection();
                rs.close();
            } catch (Exception ex) {
            }
            try {
                conn.close();
            } catch (Exception ex) {
            }
        }
        return succeedFlag;
    }

    //need upper program to release the connection
    public ResultSet executeQuery(String sql) throws Exception {
        Statement stat = getStatement();
        ResultSet rs = null;
        try {
            rs = stat.executeQuery(sql);
            return rs;
        } catch (Exception ex) {
            throw ex;
        }
    }

    public int executeUpdate(String sql) throws Exception {
        Statement stat = getStatement();

        int val = -1;
        boolean autoCommit = true;
        try {
            autoCommit = stat.getConnection().getAutoCommit();
            stat.getConnection().setAutoCommit(false);
            val = stat.executeUpdate(sql);
            stat.getConnection().commit();
        } catch (Exception ex) {
            stat.getConnection().rollback();
            ex.printStackTrace();
            val = -1;
        } finally {
            Connection conn = null;
            try {
                stat.getConnection().setAutoCommit(autoCommit);
            } catch (Exception ex) {
            }
            try {
                conn = stat.getConnection();
                stat.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                conn.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return val;
        }

    }

    public boolean executeBatchInsertFC(String sql, List params) throws Exception {
//        Debugger.info("batchInsert:" + sql);

        List<List> paramstmp = params;
        int rownum = paramstmp.get(0).size();
        int colnum = paramstmp.size();
        PreparedStatement prstat = getPreparedStatement(sql);

        try {

            for (int i = 0; i < rownum; i++) {

                for (int j = 1; j <= colnum; j++) {

                    Object o = paramstmp.get(j - 1).get(i);

                    prstat.setObject(j, o);
                }
                prstat.addBatch();
            }

            prstat.executeBatch();
            prstat.getConnection().commit();

        } catch (SQLException sqlex) {
            throw sqlex;
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                prstat.getConnection().close();
            } catch (Exception ex) {
            }
        }

        return true;
    }

    public boolean executeBatchInsertFR(String sql, List params) throws Exception {
//        Debugger.info("batchInsert:" + sql);

        List<List> paramstmp = params;
        int rownum = paramstmp.size();
        int colnum = paramstmp.get(0).size();
        PreparedStatement prstat = getPreparedStatement(sql);

        try {

            for (int i = 0; i < rownum; i++) {

                for (int j = 1; j <= colnum; j++) {

                    Object o = paramstmp.get(i).get(j - 1);

                    prstat.setObject(j, o);
                }
                prstat.addBatch();
            }

            prstat.executeBatch();
            prstat.getConnection().commit();

        } catch (SQLException sqlex) {
            throw sqlex;
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                prstat.getConnection().close();
            } catch (Exception ex) {
            }
        }

        return true;
    }

    public PreparedStatement getPreparedStatement(String sql) throws Exception {
        Connection conn = null;
        PreparedStatement prstat = null;

        try {
            conn = getConnection();
            prstat = conn.prepareStatement(sql);
            return prstat;
        } catch (Exception ex) {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception innerex) {
            }
            throw new Exception("creating prepared statement obj to current database cluster " + this.dbCluster.getName() + " is failed for " + ex, ex);
        }
    }

    public Statement getStatement() throws Exception {
        Connection conn = null;
        Statement stat = null;
        try {
            conn = getConnection();
            stat = conn.createStatement();
            return stat;
        } catch (Exception ex) {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception innerex) {
            }
            throw new Exception("creating statement obj to current database cluster " + this.dbCluster.getName() + " is failed for " + ex, ex);
        }

    }

    public synchronized Connection getConnection() throws Exception {
        Connection conn = null;

        //try some dbnode first
        try {
//            Debugger.info("connection num:" + this.cpds.toString() + ":" + this.cpds.getNumConnections() + "-" + this.cpds.getNumIdleConnections());
            int randNum = (int) (Math.random() * this.cpds.length);
            ComboPooledDataSource cpdsTmp = this.cpds[randNum];
            conn = cpdsTmp.getConnection();
            logger.debug(this.dbCluster.getName() + "-->connection num:" + cpdsTmp.getNumConnections() + "-" + cpdsTmp.getNumIdleConnections());
        } catch (Exception ex) {
            logger.debug(ex, ex.getCause());
        }

        //try all dbnodes if first try is failed
        for (int i = 0; (conn == null) && i < this.cpds.length; i++) {
            try {
                ComboPooledDataSource cpdsTmp = this.cpds[i];
                conn = cpdsTmp.getConnection();
                logger.debug(this.dbCluster.getName() + "-->connection num:" + cpdsTmp.getNumConnections() + "-" + cpdsTmp.getNumIdleConnections());
            } catch (Exception ex) {
                logger.debug(ex, ex.getCause());
                continue;
            }
        }

        if (conn != null) {
            try {
                conn.setAutoCommit(false);
                return conn;
            } catch (Exception ex) {
                ex.printStackTrace();
                try {
                    conn.close();
                } catch (Exception innerex) {
                    innerex.printStackTrace();
                }
                return null;
            }
        } else {
            throw new Exception("can't establish available connection to current database cluster " + this.dbCluster.getName());
        }
    }

    public boolean initDao() {

        try {
            if (this.dbCluster instanceof SingleDBNode || this.dbCluster instanceof RAC) {
                this.cpds = new ComboPooledDataSource[1];
                this.cpds[0] = new ComboPooledDataSource();
                setConnPool(this.cpds[0], this.dbCluster);
            } else if (this.dbCluster instanceof PARADB) {
                PARADB paraDB = (PARADB) this.dbCluster;
                List<DBCluster> dbClusters = paraDB.getDBs();
                this.cpds = new ComboPooledDataSource[dbClusters.size()];
                for (int i = 0; i < cpds.length; i++) {
                    cpds[i] = new ComboPooledDataSource();
                    setConnPool(this.cpds[i], dbClusters.get(i));
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    private void setConnPool(ComboPooledDataSource _cpds, DBCluster _dbCluster) throws Exception {

        if (_dbCluster instanceof SingleDBNode) {
            SingleDBNode singleDBNode = (SingleDBNode) _dbCluster;
            _cpds.setDriverClass(singleDBNode.getSidDriver());
            _cpds.setJdbcUrl(singleDBNode.getSidUrl());
            _cpds.setUser(singleDBNode.getSidUser());
            _cpds.setPassword(singleDBNode.getSidPasswd());
            _cpds.setMaxStatements(0);
            _cpds.setMaxPoolSize(singleDBNode.getMaxConnNum());
            _cpds.setMinPoolSize(1);
            _cpds.setInitialPoolSize(1);
            _cpds.setIdleConnectionTestPeriod(60);
            _cpds.setPreferredTestQuery("select * from dual");
            _cpds.setAcquireRetryAttempts(3);
//            _cpds.setBreakAfterAcquireFailure(true);
            _cpds.setCheckoutTimeout(500);
        } else if (_dbCluster instanceof RAC) {
            RAC rac = (RAC) _dbCluster;
            _cpds.setDriverClass(rac.getDriver());
            _cpds.setJdbcUrl(rac.getUrl());
            _cpds.setUser(rac.getUser());
            _cpds.setPassword(rac.getPasswd());
            _cpds.setMaxStatements(0);
            _cpds.setMaxPoolSize(rac.getMaxConnNum());
            _cpds.setMinPoolSize(1);
            _cpds.setInitialPoolSize(1);
            _cpds.setIdleConnectionTestPeriod(60);
            _cpds.setPreferredTestQuery("select * from dual");
            _cpds.setAcquireRetryAttempts(3);
//            _cpds.setBreakAfterAcquireFailure(true);
            _cpds.setCheckoutTimeout(500);
        }
    }

    public Dao(DBCluster _dbCluster) {

        this.dbCluster = _dbCluster;

        this.initDao();
    }

    public void clean() {
        for (int i = 0; i < cpds.length; i++) {
            cpds[i].close();
        }
    }

    public static void main(String[] args) {

        String sql = "insert into aaa(d,c) values(a,b)";
        int st = sql.indexOf("into") + 4;
        int ed = sql.indexOf('(', st);
        System.out.println(sql.substring(st, ed).trim());

        //Set<String> fpset = Constant.fingerMap.get(sql.substring(st, ed).trim());

    }
}
