/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.api;


import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author szydlowskidom
 */
public class DataSource {

    private static  List<DataSource>  datasources = new ArrayList<DataSource>(); ;
    private ComboPooledDataSource cpds;
    static final Logger logger =  LogManager.getLogger(DataSource.class);
       
    private DataSource(JdbcApi jdbc) {
        try { 
            cpds = new ComboPooledDataSource();
                     
            cpds.setDriverClass("com.sybase.jdbc4.jdbc.SybDriver"); //loads the jdbc driver
            
         
            cpds.setJdbcUrl("jdbc:sybase:Tds:"+jdbc.getHost()+":" + jdbc.getPort());
            
           
            cpds.setUser(jdbc.getUser());
            cpds.setPassword(jdbc.getPassword());
            cpds.setLoginTimeout(jdbc.getLoginTimeout());
           // cpds.setIdleConnectionTestPeriod(3600);
            cpds.setDataSourceName(jdbc.getInterfaceName());

            // the settings below are optional -- c3p0 can work with defaults
            cpds.setMinPoolSize(jdbc.getMinPoolSize());
            cpds.setAcquireIncrement(jdbc.getAcquireIncrement());
            cpds.setMaxPoolSize(jdbc.getMaxPoolSize());
            cpds.setMaxStatements(jdbc.getMaxStatements());
            cpds.setMaxIdleTime(300);
            cpds.setInitialPoolSize(jdbc.getMinPoolSize());
            cpds.setPreferredTestQuery("SELECT 1");
          
            Properties prop = new Properties(); 
            prop.setProperty("USER", jdbc.getUser());
            prop.setProperty("PASSWORD", jdbc.getPassword());
            prop.setProperty("APPLICATIONNAME", "thread.pool.sybase");
            prop.setProperty("DEFAULT_QUERY_ TIMEOUT", "20");
            prop.setProperty("DELETE_WARNINGS_FROM_EXCEPTION_CHAIN", "true");
            
            
           
            cpds.setProperties(prop);
                        
        } catch (Exception e){
            logger.error(e);
        }

    }

    public static void addInstance(JdbcApi jdbc) throws IOException, SQLException, PropertyVetoException {
         datasources.add(new DataSource(jdbc));
       
    }  
   
    public static void retry (JdbcApi jdbc) throws IOException, SQLException, PropertyVetoException {
            datasources.set(jdbc.getPoolIndex(), new DataSource(jdbc));
       
    }
      
    public static DataSource getInstance(int i)  {
        if (i< datasources.size()) {
           return datasources.get(i);
        } else return null;
      
    }

    public Connection getConnection() {
        try {
            return this.cpds.getConnection();
        } catch (Exception e){
            return null;
        }
    }   
    
    public void closePool() {
        try {
           this.cpds.close(); 
        } catch (Exception e){
        }
    }
    
    
    //*****************************************************************************************************
    
    private int checkSybaseConn(String host, String port, String user, String password, int timeout){
      
        int _port = 5000;
        
        try {
           _port = Integer.parseInt(port);
        } catch (Exception e) {}
        
        
        Connection _conn = getConnectionRetry(host, _port, user, password, timeout);  

         if (_conn == null){
             return 0;
             // tu nie ma byc isError
         } else {
               try {
                        
                        Statement stmt = _conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT 1");
                   

                        while (rs.next())  {
                            rs.getString(1); 
                        }

                        attemptClose(rs);
                        attemptClose(stmt);

                } 
                catch (SQLException e) {
                       logger.error("SQL Exception/Error:");
                       logger.error("error message=" + e.getMessage());
                       logger.error("SQL State= " + e.getSQLState());
                       logger.error("Vendor Error Code= " + e.getErrorCode());
                       logger.error("SQL Exception/Error: ", e);
                       return -200;
                }  finally {
                     attemptClose(_conn);
                }

             return 1;
         }
         
         
    }    
    
    private Connection getConnectionRetry(String host, int port, String user, String password, int timeout) {
        int licz_conn=1;
        Connection connection =null;
        
        while (licz_conn<=2){
           connection = getConnection(host, port, user, password, timeout);
           if (connection==null){
               logger.error("Conn is null RETRY " + licz_conn);
              //po nowemu retry
               try {
                   Thread.sleep(1000);
               } catch (InterruptedException ex) {
                   logger.error(ex);
               }
                licz_conn++;
            } else {
                licz_conn=4;
                break;
            }
        }
        
        return connection;
  }//tylko dla compare
   private Connection getConnection(String host, int port, String user, String password, int timeout) {
           Properties prop = new Properties();

           prop.setProperty("USER", user);
           prop.setProperty("PASSWORD", password);
           prop.setProperty("APPLICATIONNAME", "thread.single.sybase");
       
                     
           try {
               Class.forName("com.sybase.jdbc4.jdbc.SybDriver");
               DriverManager.setLoginTimeout(timeout);
               Connection connection =  DriverManager.getConnection("jdbc:sybase:Tds:"+host+":" + port, prop);
           if(connection!=null){
               logger.debug("Connected to " + host+":"+port);
           }
             return connection;
          } catch (ClassNotFoundException e) {
                 logger.error("ClassNotFoundException:");
                 logger.error("error message=" + e.getMessage());
                 return null;

          } catch (SQLException e) {
              while(e != null) {
                            logger.error("SQL Exception/Error:");
                            logger.error("error message=" + e.getMessage());
                            logger.error("SQL State= " + e.getSQLState());
                            logger.error("Vendor Error Code= " + e.getErrorCode());

                            // it is possible to chain the errors and find the most
                            // detailed errors about the exception
                            e = e.getNextException( );
               }
               return null;
          } catch (Exception e2) {
            // handle non-SQL exception …
              logger.error("SQL Exception/Error:");
              logger.error("error message=" + e2.getMessage());

               return null;
          }
          

     }    
   
   static void attemptClose(ResultSet o)  {
	try
	    { if (o != null) o.close();}
	catch (SQLException e)
	    {
               logger.error(e);
            }
    }

    static void attemptClose(Statement o) {
	try
	    { if (o != null) o.close();}
	catch (SQLException e)
	    {
                logger.error(e);
            }
    }

    static void attemptClose(Connection o) {
	try
	    { if (o != null) o.close();}
	catch (SQLException e)
	    { 
               logger.error(e);
            }
    }
    


}