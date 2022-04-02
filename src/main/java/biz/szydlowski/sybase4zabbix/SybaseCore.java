/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;


import static biz.szydlowski.sybase4zabbix.WorkingObjects.*;
import biz.szydlowski.sybase4zabbix.api.DataSource;
import biz.szydlowski.utils.WorkingStats;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
select db_name(dbid) from master..sysusages group by dbid
*/
/**
 *
 * @author szydlowskidom
 */
public class SybaseCore {
    
     static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.SybaseCore"); 
    String newline = System.getProperty("line.separator");
    public static final String SYBASE_VALIDATION_QUERY = "SELECT 1";
    
    private long executeTime;
    private int __queue=0;
    private boolean isError=false; 
    private String errorDescr="";
    
    Connection conn = null;
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.GERMAN);
      
    private String returnString(double v){
        otherSymbols.setDecimalSeparator('.');
        otherSymbols.setGroupingSeparator('.'); 
        DecimalFormat df = new DecimalFormat("###.######", otherSymbols);
        return df.format(v);
    }
    
    public long getExecuteTime(){
        return executeTime;
    }
  
    public String doAndReturnData(int _queue, boolean open_conn, boolean close_conn){
       
        executeTime = 0L;
       
        this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return "error";
        }
        
        String s_return="default";
        isError=false;      
                  
        if (null!=sybaseApiList.get(__queue).getSqlType())  switch (sybaseApiList.get(__queue).getSqlType()) {
            case MAINTENANCE:
                switch (sybaseApiList.get(__queue).getSqlQuery()) {
                   
                    case "connError":
                        s_return = ""+WorkingStats.getConnError();
                        break;
                    
                    case "activeThreads":
                        s_return = ""+WorkingStats.getActiveThreads();
                        break; 
                    
                    case "javaUsedMemory":
                        s_return = ""+WorkingStats.getJavaUsedMemory();
                        break;
                        
                    case "okCount":
                        s_return = ""+WorkingStats.getOkCount();
                        break;
                        
                    case "errorCount":
                        s_return = ""+WorkingStats.getErrorCount();
                        break;
                        
                    case "testsCount":
                        s_return = ""+WorkingStats.getTestsCount();
                        break;
                        
                    case "uptime":
                        s_return = ""+WorkingStats.getUptime();
                        break; 
                    
                    case "idletime":
                        s_return = ""+WorkingStats.getIdleTime();
                        break;
                    
                    case "lockCount":
                        s_return = ""+WorkingStats.getLockCount();
                        break; 
                    
                    case "lockAttempt":
                        s_return = ""+WorkingStats.getLockAttempt();
                        break;
                        
                    case "zabbixProcessed":
                        s_return = ""+WorkingStats.getZabbix_processed();
                        break;
                        
                    case "zabbixFailed":
                        s_return = ""+WorkingStats.getZabbix_failed();
                        break;
                        
                    case "zabbixTotal":
                        s_return = ""+WorkingStats.getZabbix_total();
                        break;  
                    
                    case "autorestart":
                        s_return = ""+WorkingStats.getAutorestartCount();
                        break; 
                        
                    case "version":
                        s_return = ""+Version.getAgentVersion();
                        break;
                        
                    default:
                        logger.fatal("MAINTENANCE " + sybaseApiList.get(__queue).getSqlQuery());
                        s_return  = "-210";
                        isError=true;
                        errorDescr = "MAINTENANCE UNKNOWN FUNCTION";
                }   break;
           case DB_MAINTENANCE:  
               String interface_tmp = sybaseApiList.get(__queue).getSybaseInterface();
               long long_tmp=0L;
              
               switch (sybaseApiList.get(__queue).getSqlQuery()) {
                   
                   case "queryHTSRatio":
                        interface_tmp = sybaseApiList.get(__queue).getSybaseInterface();
                        long long_tmp_ts=0L;
                        long long_tmp_hits=0L;
                        
                        for (int i=0; i<sybaseApiList.size(); i++){
                            sqlType type = sybaseApiList.get(i).getSqlType();

                           if (type != sqlType.SYSMON && type != sqlType.SYSMON_WITH_DISCOVERY ){  
                               if (sybaseApiList.get(i).getSybaseInterface().equals(interface_tmp)){
                                    long_tmp_ts =  long_tmp_ts+sybaseApiList.get(i).getTimeSpendingDt();
                                    long_tmp_hits = long_tmp_hits+sybaseApiList.get(i).getNumberOfHitsDt();
                                    sybaseApiList.get(i).resetDtCounter();
                               } else {
                               }
                           }
                        }
                        
                        for (int i=0; i<sysmonApiList.size(); i++){
                             if (sysmonApiList.get(i).getSybaseInterface().equals(interface_tmp)){
                                 long_tmp_ts =  long_tmp_ts+sysmonApiList.get(i).getTimeSpendingDt();
                                 long_tmp_hits = long_tmp_hits+sysmonApiList.get(i).getNumberOfHitsDt();
                                 sysmonApiList.get(i).resetDtCounter();
                                 break;
                             }
                            
                        }
                        
                        
                        if (long_tmp_hits==0)  s_return = "0";
                        else s_return = returnString(long_tmp_ts/(1.0*long_tmp_hits));
                        
                        break;
                  
                    case "queryTimeSpending":
                        interface_tmp = sybaseApiList.get(__queue).getSybaseInterface();
                        long_tmp=0L;
                        
                        for (int i=0; i<sybaseApiList.size(); i++){
                            sqlType type = sybaseApiList.get(i).getSqlType();

                           if (type != sqlType.SYSMON && type != sqlType.SYSMON_WITH_DISCOVERY ){  
                               if (sybaseApiList.get(i).getSybaseInterface().equals(interface_tmp)){
                                   long_tmp = long_tmp+sybaseApiList.get(i).getTimeSpending();
                               } else {
                               }
                           }
                        }
                        
                        for (int i=0; i<sysmonApiList.size(); i++){
                             if (sysmonApiList.get(i).getSybaseInterface().equals(interface_tmp)){
                                 long_tmp = long_tmp+sysmonApiList.get(i).getTimeSpending();
                                 break;
                             }
                            
                        }
                        
                        s_return = ""+long_tmp;
                        break;     
                    
                    case "queryHits":
                        interface_tmp = sybaseApiList.get(__queue).getSybaseInterface();
                        long_tmp=0L;
                        
                        for (int i=0; i<sybaseApiList.size(); i++){
                           sqlType type = sybaseApiList.get(i).getSqlType();

                           if (type != sqlType.SYSMON && type != sqlType.SYSMON_WITH_DISCOVERY ){  
                               if (sybaseApiList.get(i).getSybaseInterface().equals(interface_tmp)){
                                 long_tmp = long_tmp+sybaseApiList.get(i).getNumberOfHits();
                               }
                           }
                        
                        }  
                        
                        for (int i=0; i<sysmonApiList.size(); i++){
                                 if (sysmonApiList.get(i).getSybaseInterface().equals(interface_tmp)){
                                     long_tmp = long_tmp+sysmonApiList.get(i).getNumberOfHits();
                                     break;
                                 }
                        }
                        
                        s_return = ""+long_tmp;
                        break;  
               } break;
        // koniec wbudowanego
            case EMBEDDED:
            case EMBEDDED_WITH_DISCOVERY:
                boolean percentage = Boolean.parseBoolean( sybaseApiList.get(__queue).getDatabaseProperty("percentage", "true"));
                int check=0; 
                String database, queue, type, table, system_view, ipaddr;
                switch (sybaseApiList.get(__queue).getSqlQuery()) {  
                   
                    case "rep_queues":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        database = sybaseApiList.get(__queue).getDatabaseProperty("database", "rssd");
                        queue = sybaseApiList.get(__queue).getDatabaseProperty("queue", "total");
                        type = sybaseApiList.get(__queue).getDatabaseProperty("type", "total");
                        if (logger.isDebugEnabled()) logger.debug("database: " + database + ", queue: " + queue+ ", type: " + type);
                        
                        double kol = 1.0*rs_kol(database, queue, type);
                        s_return = ""+convertSpaceFromMegaBytes(kol, sybaseApiList.get(__queue).getDatabaseProperty("unit", "M"));
                        attemptClose(conn);
                        break;   
                    
                    case "rep_admin_health":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        s_return = admin_health();
                        attemptClose(conn);
                        break;
                        
                    case "rep_admin_who_is_down":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        String t1 = sybaseApiList.get(__queue).getDatabaseProperty("type", "all");
                        kol = admin_who_is_down(t1);
                        s_return = kol+"";
                        attemptClose(conn);
                        break; 
                    
                    case "rep_admin_who_count":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        kol = admin_who_count();
                        s_return = kol+"";
                        attemptClose(conn);
                        break;
                        
                    case "rep_admin_who_is_up":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        String t2 = sybaseApiList.get(__queue).getDatabaseProperty("type", "all");
                        kol = admin_who_is_up(t2);
                        s_return = kol+"";
                        attemptClose(conn);
                        break;
                        
                    case "rep_admin_disk_space_used":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                       if (percentage){
                            s_return = returnString(admin_disk_space(percentage, true));
                        } else {
                            s_return =  convertSpaceFromMegaBytes(admin_disk_space(percentage, true), sybaseApiList.get(__queue).getDatabaseProperty("unit", "M"));
                        }
                        attemptClose(conn);
                        break;
                        
                    case "rep_admin_disk_space_free":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        if (percentage){
                            s_return = returnString(admin_disk_space(percentage, false));
                        } else {
                            s_return =  convertSpaceFromMegaBytes(admin_disk_space(percentage, false), sybaseApiList.get(__queue).getDatabaseProperty("unit", "M"));
                        }
                        attemptClose(conn);
                        break;
                        
                    case "sybase_db_space":
                        if (open_conn) {
                            conn = getConnectionRetry();
                        } else  if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool") ) conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        database = sybaseApiList.get(__queue).getDatabaseProperty("database", "master");
                        type = sybaseApiList.get(__queue).getDatabaseProperty("type", "type");
                    
                        boolean free =  Boolean.parseBoolean(sybaseApiList.get(__queue).getDatabaseProperty("free", "true"));
                        boolean allInOne =  Boolean.parseBoolean(sybaseApiList.get(__queue).getDatabaseProperty("allInOne", "false"));
                        
                        if (logger.isDebugEnabled()) logger.debug("database="  + database + ", type="+type);
                                                
                        if (allInOne){
                            double[] space = new double[4];
                            //free, free-prc, used, used-prc
                            logger.debug("allInOne " + type +"  " + database);
                            space = sp_helpsegment_exc(type, database, percentage, free, true);
                            String u = sybaseApiList.get(__queue).getDatabaseProperty("unit", "M");
                            s_return = convertSpaceFromMegaBytes(space[0], u) + ";" + space[1]+";"+
                                    convertSpaceFromMegaBytes(space[2], u) + ";"  + space[3]; 
                        } else {
                             if (type.equalsIgnoreCase("data") || type.equalsIgnoreCase("system") || type.equalsIgnoreCase("log")){
                                double[] space = new double[1];
                                space[0] = sp_helpsegment_exc(type, database, percentage, free, false)[0];

                                if (percentage) s_return = returnString(space[0]);
                                else  s_return =  convertSpaceFromMegaBytes(space[0], sybaseApiList.get(__queue).getDatabaseProperty("unit", "M"));
                             } else {
                                s_return = "error";
                                isError=true;
                                errorDescr = "sybase_db_space unknown function " + type;
                             }

                        }
                        if (close_conn) attemptClose(conn);
                        else if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool")) attemptClose(conn);
                        break;
                   
                    case "sybase_table_space":
                        if (open_conn) {
                            conn = getConnectionRetry();
                        } else  if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool") ) conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        database = sybaseApiList.get(__queue).getDatabaseProperty("database", "master");
                        table = sybaseApiList.get(__queue).getDatabaseProperty("table", "table");
                                            
                        if (logger.isDebugEnabled()) logger.debug("database="  + database + ", table="+table);
                                                
                                                   
                        double[] space = sp_spaceused_exc(database, table);
                        s_return =space[0]+ ";"+space[1]+";"+space[2]+ ";"+ space[3]+";"+ space[4]+";"+ space[5]; 
                        
                        if (close_conn) attemptClose(conn);
                        else if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool")) attemptClose(conn);
                        break;
                        
                    case "max_time_blocked_for_ipaddr":
                        if (open_conn) {
                            conn = getConnectionRetry();
                        } else  if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool") ) conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
      
                        system_view = sybaseApiList.get(__queue).getDatabaseProperty("system_view", "default");
                        if (system_view.equalsIgnoreCase("default")){
                            logger.info("max_time_blocked_for_ipaddr system_view");    
                            system_view = getQueryHelp("select @@system_view");
                            sybaseApiList.get(__queue).setDatabaseProperty("system_view", system_view);
     
                        }
                        
                        ipaddr = sybaseApiList.get(__queue).getDatabaseProperty("ipaddr", "ipaddr");
                                            
                        if (logger.isDebugEnabled()) logger.debug("ipaddr="  + ipaddr);
                                             
                        
                        s_return = ""+max_time_blocked_for_ipaddr(ipaddr, system_view);
                        
                        if (close_conn) attemptClose(conn);
                        else if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool")) attemptClose(conn);
                        break; 
                    
                    case "count_blocked_for_ipaddr":
                        if (open_conn) {
                            conn = getConnectionRetry();
                        } else  if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool") ) conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
      
                        system_view = sybaseApiList.get(__queue).getDatabaseProperty("system_view", "default");
                       
                        if (system_view.equalsIgnoreCase("default")){
                            logger.info("count_blocked_for_ipaddr system_view");    
                            system_view = getQueryHelp("select @@system_view");
                            sybaseApiList.get(__queue).setDatabaseProperty("system_view", system_view);
     
                        }
                        
                        ipaddr = sybaseApiList.get(__queue).getDatabaseProperty("ipaddr", "ipaddr");
                                            
                        if (logger.isDebugEnabled()) logger.debug("ipaddr="  + ipaddr);
                                             
                        
                        s_return = Integer.toString(count_blocked_for_ipaddr(ipaddr, system_view));
                        
                        if (close_conn) attemptClose(conn);
                        else if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("pool")) attemptClose(conn);
                        break;
                        
                    case "sybase_connection":
                        check = checkSybaseConn() ;
                        if (check==200) s_return = "CONN";
                        if (check==-200) s_return = "NO_CONN";
                        break;
                        
                    case "sybase_replication_connection":
                        check = checkSybaseRepConn();
                        if (check==200) s_return = "CONN";
                        if (check==-200) s_return = "NO_CONN";
                        break;
                        
                        
                    case "sybase_connection_numeric":
                        check = checkSybaseConn();
                        if (check==200) s_return = "1";
                        if (check==-200) s_return = "0";
                        break;
                        
                    case "sybase_replication_connection_numeric":
                        check = checkSybaseRepConn();
                        if (check==200) s_return = "1";
                        if (check==-200) s_return = "0";
                        break;
                        
                        
                        
                    case "sybase_connection_time":
                        long startTime = System.currentTimeMillis();
                        checkSybaseConn();
                        long endTime = System.currentTimeMillis() - startTime;
                        s_return  = "" + endTime;
                        break;
                        
                        
                    case "sybase_kill":
                        conn = getConnectionRetry();
                        if (conn==null){
                            isError = true;
                            errorDescr = "Could not create connection to database server";
                            return "0";
                        }
                        s_return  =  "" + killPIDs();
                        //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single"))
                        attemptClose(conn);
                        break;
                        
                        
                    default:
                        isError=true;
                        errorDescr = "Unknown Embedded function";
                        s_return  = "-215";
                        
                }   break;
            case PREDEFINED:
            case PURE:
                conn = getConnectionRetry();
                if (conn==null){
                    isError = true;
                    errorDescr = "Could not create connection to database server";
                    return "0";
                }   if (sybaseApiList.get(__queue).isTiming()){
                    
                    long startTime = System.currentTimeMillis();
                    doQuery();
                    long endTime = System.currentTimeMillis() - startTime;
                    if (logger.isDebugEnabled()) logger.debug("timing time " + endTime);
                    s_return  = "" + endTime;
               } else  if (sybaseApiList.get(__queue).isFullQuery()){ 
                    //moze byc data lub nodata
                     s_return = doQuery();
                } else  if (sybaseApiList.get(__queue).isInteger() || sybaseApiList.get(__queue).isFloat() || sybaseApiList.get(__queue).isLong()){
                    s_return = doNumericQuery();
                }  else {
                    s_return = doStringQuery();
                    
                }   
                //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single")) 
                attemptClose(conn);
                break;
            case COMPARE:
                s_return = doQueryCompare();
                break;
            default:
                break;
        }
        
        
       if (logger.isDebugEnabled()) logger.debug("PROCESSING " + sybaseApiList.get(__queue).getSybaseHost() + ":" + sybaseApiList.get(__queue).getSybasePort() + " DONE");
       return  s_return;
         
    }
    
    public String getErrorDescription(){
        return errorDescr;
    }
   
    private int rs_kol(String database, String queue, String type){
        
        int ret=0;
         
                  
        StringBuilder query1 = new StringBuilder();
         
         query1.append("select distinct dsname + '.' + dbname as Polaczenie,  'Inbound' as Typ_kolejki,  0 as Kolejka, number, q.state, q.type as Typ_kolejki_sym")
                .append(" from rs_queues q, rs_databases d")
                .append(" where number = d.dbid and type=1"); 

         
        query1.append(" select distinct  isnull(convert(varchar(61), name), dsname+'.'+dbname), 'Outbound' as Typ_kolejki, 0 as Kolejka, number, q.state, q.type as Typ_kolejki_sym")	 
         .append(" from rs_queues q, rs_databases, rs_sites") 		 
         .append(" where number *= dbid")			 
         .append(" and number *= id and type=0");
   
         List<List<String>> results = new ArrayList<>();
               
         int i, j=0;
         boolean hasMoreResults = false;
         boolean isResult = false;
         ResultSet rs = null;
         ResultSetMetaData rsmd = null;
           
         try {
           executeTime = System.currentTimeMillis();
           conn.setCatalog(database);
                      
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           isResult = stmt.execute(query1.toString());
           int updates = 0;


           do {
             if (isResult){
                rs = stmt.getResultSet();
                rsmd = rs.getMetaData();
                
                int numCols = rsmd.getColumnCount ();             
                
                while (rs.next())  {
                      List<String> row = new ArrayList<>();
                      for (i=1; i<=numCols; i++) {
                          String tmp = rs.getString(i);
                          if (rs.wasNull()) {
                             tmp="0";
                          } else {
                          }
                          row.add(tmp);
                          //results[i][j]=tmp;
                      }
                      results.add(row);
                      //j++;
                }
                int rowsSelected = stmt.getUpdateCount();
                if (rowsSelected >= 0){
                    //  if (logger.isDebugEnabled()) logger.debug(rowsSelected + " ROWS_AFFECTED");
                }
            } //end if  
            
            else {
               updates = stmt.getUpdateCount();
            }
             hasMoreResults = stmt.getMoreResults();
             isResult = hasMoreResults;
           } //end do
           while ((hasMoreResults) || (updates != -1));
           
           attemptClose(rs);
           attemptClose(stmt);
        
        }   
        
       
         
        catch (SQLException e) {
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError = true;
               errorDescr = e.getMessage();
               return 0;
        } finally {
              executeTime = System.currentTimeMillis()-executeTime;
        }
         
         
       //int max_row = j; 
      
       
       String _queue;
       String _type;
       String _value;
       ret = 0;
        
       for (j=0;j<results.size();j++){
              results.get(j).set(2, rs_kol_helper(conn, results.get(j).get(3), results.get(j).get(5)));
     
              _queue = results.get(j).get(0);
              _type = results.get(j).get(1);
              _value = results.get(j).get(2);
              
              if (queue.equalsIgnoreCase(_queue)){
                  if (type.equalsIgnoreCase(_type)){
                      ret = ret + Integer.parseInt(_value);
                  }
              }
              
              if (queue.equalsIgnoreCase(_queue)){
                  if (type.equalsIgnoreCase("total")){
                      ret = ret + Integer.parseInt(_value);
                  }
              }
              
              if (queue.equalsIgnoreCase("total")){
                  if (type.equalsIgnoreCase(_type)){
                      ret = ret + Integer.parseInt(_value);
                  }
              }
              
              if (queue.equalsIgnoreCase("total")){
                  if (type.equalsIgnoreCase("total")){
                      ret = ret + Integer.parseInt(_value);
                  }
              }
         }
       
        results.clear();
        results = null;
         
        return ret;
    }
     
     
     private String rs_kol_helper(Connection conn, String number, String Typ_kolejki_sym){
        
        StringBuilder query = new StringBuilder();
        query.append("select count(*) from rs_segments").append(" where ").append(number)
        .append(" = rs_segments.q_number").append(" and ")		 
	.append(Typ_kolejki_sym)	 
  	.append(" = rs_segments.q_type")	 
  	.append(" and used_flag > 0");       
      
         if (conn == null){
            logger.error("Could not create connection to database server  mj_kol_helper");
            isError = true;
            errorDescr = "Could not create connection to database server  mj_kol_helper";
            return "error";
         }
        
         ResultSet rs = null;
         String rs_text="";
         
         try {
           
           executeTime = System.currentTimeMillis();  
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           rs = stmt.executeQuery(query.toString());
         
           //logger.debug("Execute query: " + query);
       
           while (rs.next())  {
               
               rs_text = rs.getString(1);
               if (rs.wasNull()) {
                  rs_text="0";
               } else {
               }
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
               isError = true;
               errorDescr = e.getMessage();
               return "error";
        } finally {
               executeTime = System.currentTimeMillis()-executeTime;
        }
     
        return rs_text;
    }
     
    private int admin_who_is_down(String type){
        String query ="admin who_is_down"; 
   
         if (conn == null){
             logger.error("Could not create connection to database server admin_who_is_down");
             isError = true;
             errorDescr = "Could not create connection to database server admin_who_is_down";
             return -200;
         }
        
      
         int ile_bad=0;
                  
         try {
                executeTime = System.currentTimeMillis();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs = stmt.executeQuery(query);
                //logger.debug("Execute query: " + query);
                       
                
                while (rs.next())  {
                     if (type.equalsIgnoreCase("all")) {
                         ile_bad++;
                     } else {
                         String typtemp = rs.getString(2);
                         if (rs.wasNull()) {
                             typtemp="NULL";
                         } else {
                         }
                         if (typtemp.equalsIgnoreCase(type)) ile_bad++;
                     }
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
               isError=true;
               errorDescr = e.getMessage();
               return -205;
        } finally {
               executeTime = System.currentTimeMillis()-executeTime;
        }
        
        return ile_bad;
    }
    
    private String admin_health(){
        String query ="admin health"; 
        String ret="DEFAULT";
        
         if (conn == null){
             logger.error("Could not create connection to database server admin health");
             isError = true;
             errorDescr = "Could not create connection to database server admin health";
             return "ERROR";
         }
        
      
     
                  
         try {
                executeTime = System.currentTimeMillis();
                
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs = stmt.executeQuery(query);
                //logger.debug("Execute query: " + query);
                       
                
                while (rs.next())  {
                    ret = rs.getString(3);
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
               isError=true;
               errorDescr = e.getMessage();
               ret = "SQL Exception";
        } finally {
               executeTime = System.currentTimeMillis()-executeTime;
        }
        
        return ret;
    }
    
    private int admin_who_is_up(String type){
        String query ="admin who_is_up"; 
   

         if (conn == null){
             logger.error("Could not create connection to database server admin_who_is_up");
             isError = true;
             errorDescr = "Could not create connection to database server admin_who_is_up";
             return -200;
         }
        
      
         int ile_bad=0;
                  
         try {
                executeTime = System.currentTimeMillis();
                  
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs = stmt.executeQuery(query);
                //logger.debug("Execute query: " + query);
                       
                
                while (rs.next())  {
                     if (type.equalsIgnoreCase("all")) {
                         ile_bad++;
                     } else {
                         String typtemp = rs.getString(2);
                         if (rs.wasNull()) {
                             typtemp="NULL";
                         } else {
                         }
                         if (typtemp.equalsIgnoreCase(type)) ile_bad++;
                     }
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
               isError=true; 
               errorDescr = e.getMessage();
               return -205;
        } finally {
               executeTime = System.currentTimeMillis()-executeTime;
        }
        
        return ile_bad;
    }
    
    private int admin_who_count(){
        String query ="admin who"; 
   

         if (conn == null){
             logger.error("Could not create connection to database server admin_who");
             isError = true;
             errorDescr = "Could not create connection to database server admin_who";
             return -200;
         }
        
      
         int ile=0;
                  
         try {
                executeTime = System.currentTimeMillis();
                  
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs = stmt.executeQuery(query);
                //logger.debug("Execute query: " + query);
                       
                
                while (rs.next())  {
                         ile++;
                  
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
               isError=true; 
               errorDescr = e.getMessage();
               return -205;
        } finally {
               executeTime = System.currentTimeMillis()-executeTime;
        }
        
        return ile;
    }
    
    private double admin_disk_space (boolean prc, boolean used){
        String query ="admin disk_space"; 
  

         if (conn == null){
             logger.error("Could not create connection to database server admin_disk_space");
             isError=true;  
             errorDescr = "Could not create connection to database server admin_disk_space";
             return -200.0;
         }
        
         int i;
         int total_seg=0;
         int free_seg=0;
         int used_seg=0;
         String status="";
               
         try {
                executeTime = System.currentTimeMillis();
                  
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery(query);
              //  logger.debug("Execute query: " + query);
         
                
                while (rs.next())  {
                       status =  rs.getString(6); 
                       if (rs.wasNull()) {
                             status="NULL";
                       } else {
                       } 
                       
                       if (status.contains("ON-LINE")){
                           total_seg = total_seg + rs.getInt(4);
                           used_seg = used_seg + rs.getInt(5);
                           free_seg=free_seg+(rs.getInt(4)-rs.getInt(5));
                       } else if (status.contains("DROPPED")){
                           total_seg = total_seg + rs.getInt(5);
                           used_seg = used_seg + rs.getInt(5);
                           //bez zmian free_seg 
                       }
                       
                    
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
               isError=true;  
               errorDescr = e.getMessage();
               return -205;
        }   finally {
              executeTime = System.currentTimeMillis()-executeTime;
        }
       
        if (used){
              if (prc) return 100.0*(used_seg)/total_seg;
              else return used_seg;
        }  else {
              if (prc) return 100.0*(free_seg)/total_seg;
             else return free_seg;
        }
      
        
       
    }
    
    private int checkSybaseConn(){
      
         Connection _conn = getConnectionRetry();  

         if (_conn == null){
             return -200;
             // tu nie ma byc isError
         } else {
               try {
                        executeTime = System.currentTimeMillis();
                        
                        Statement stmt = _conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                        ResultSet rs = stmt.executeQuery(SYBASE_VALIDATION_QUERY);
                   

                        while (rs.next())  {
                            rs.getString(1); 
                        }

                        attemptClose(rs);
                        attemptClose(stmt);

                } 
                catch (SQLException e) {
                       logger.error("EX2001");
                       logger.error("SQL Exception/Error:");
                       logger.error("error message=" + e.getMessage());
                       logger.error("SQL State= " + e.getSQLState());
                       logger.error("Vendor Error Code= " + e.getErrorCode());
                       logger.error("SQL Exception/Error: ", e);
                       return -200;
                }  finally {
                     attemptClose(_conn);
                     executeTime = System.currentTimeMillis()-executeTime;
                }

             return 200;
         }
         
         
    }
     
    private int checkSybaseRepConn(){
       
         Connection _conn = getConnection(); 
         int ret = -200;

         if (_conn == null){
             ret= -200;
         } else { 
             ////#BUG20181013, new echo
             try {
                        executeTime = System.currentTimeMillis();
                        
                        Statement stmt = _conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                        ResultSet rs = stmt.executeQuery("admin echo, hello");
                   
                        String hello="";
                        while (rs.next())  {
                            hello = rs.getString(1); 
                        }

                        attemptClose(rs);
                        attemptClose(stmt);
                        hello = hello.replaceAll("\\s+", "");
                        
                        if (hello.equals("hello")){
                            ret= 200;
                        }
                        

               }  catch (SQLException e) {
                      logger.error("EX2002");
                       logger.error("SQL Exception/Error:");
                       logger.error("error message=" + e.getMessage());
                       logger.error("SQL State= " + e.getSQLState());
                       logger.error("Vendor Error Code= " + e.getErrorCode());
                       logger.error("SQL Exception/Error: ", e);
                       ret = -200;
               }  finally {
                     executeTime = System.currentTimeMillis()-executeTime;
               }

             
             attemptClose(_conn);
         }
         
         return ret;
         
    } 
       
      
  
     private double [] sp_helpsegment_exc (String type, String database, boolean prc, boolean free, boolean allInOne){
      
       double[] space = new double[4];
       space[0] =0.0;
       space[1] =0.0;
       space[2] =0.0;
       space[3] =0.0;
        
       String query="select 1";
      
       if (type.equalsIgnoreCase("system")){
           query="sp_helpsegment 'system'";
       } else if  (type.equalsIgnoreCase("data") || type.equalsIgnoreCase("default")){
           query="sp_helpsegment 'default'";
       } else if  (type.equalsIgnoreCase("log")){
           query="sp_helpsegment 'logsegment'";
       }
       
        if (conn == null){
            logger.fatal("Could not create connection to database server " + query);
            isError=true;
            errorDescr = "Could not create connection to database server " + query;
            return space;
        }
        
                    
     
        try {
                executeTime = System.currentTimeMillis();
                
                conn.setCatalog(database);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                
                
               // DatabaseMetaData _DatabaseMetaData = conn.getMetaData();    
                Statement stmt = conn.createStatement();
                
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                       
                 int updates = 0;
                SQLWarning sqlW = null;  
                
                isResult = stmt.execute(query);
                         
                do {
                  
                   
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                          stmt.clearWarnings();
                    } 
                    
                   if (isResult) {
                     
                         rs = stmt.getResultSet();
                         rsmd = rs.getMetaData();   
                         int numCols = rsmd.getColumnCount ();
       
                         while (rs.next()) {
                               
                                if (numCols==5) {
                                    if (allInOne){ 
                                        space = dispSpace_AllInOne(rs);                                     
                                    } else {
                                        space[0] = dispSpace_exc(rs, prc,free);
                                    }
                                } 
                        }
                      
                          
                       
                     int rowsSelected = stmt.getUpdateCount();
                     if (rowsSelected >= 0){
                           //logger.debug(rowsSelected + "  row(s) affected");
                     }
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {

                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while ((hasMoreResults) || (updates != -1));
             
               attemptClose(rs);
               attemptClose(stmt);
               //powrót do mastera
               conn.setCatalog("master");
        } 
        catch (SQLException e) { 
               logger.error("EX2003");
               logger.error(type + " " + database + " " + prc + " " + free + " " + allInOne);
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               space[0]=0.0;
               space[1]=0.0;
               space[2]=0.0;
               space[3]=0.0;
               isError=true;
               errorDescr = e.getMessage();
       } finally {
             executeTime = System.currentTimeMillis()-executeTime;
        }
        if (space[0]<0) {
            isError=true; 
            errorDescr = "[E0001] Problem with column counter";
        }
        return space;

   }
 
     public int count_blocked_for_ipaddr (String ipaddr,  String system_view){
                    
       String query="select -1";
       int timemax=0;
       
       
        if (system_view.equalsIgnoreCase("cluster")){
           query="select  count(1) from master..sysprocesses where suid > 0 and spid != @@spid and spid!=0 and status = 'lock sleep' and blocked in (select spid from master..sysprocesses where ipaddr = '"+ipaddr+"' and instanceid=@@instanceid) at isolation 0";
        } else {
           query="select count(1) from master..sysprocesses where suid > 0 and spid != @@spid and spid!=0 and status = 'lock sleep' and blocked in (select spid from master..sysprocesses where ipaddr = '"+ipaddr+"') at isolation 0";
        }
     
        if (conn == null){
            logger.fatal("Could not create connection to database server " + query);
            isError=true;
            errorDescr = "Could not create connection to database server " + query;
            return timemax;
        }
        
        
        try {
                executeTime = System.currentTimeMillis();
                
                conn.setCatalog("master");
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                
              //  DatabaseMetaData _DatabaseMetaData = conn.getMetaData();    
                Statement stmt = conn.createStatement();
                
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                                  
                isResult = stmt.execute(query);
           
                do {
                
               
                   if (isResult) {
                     
                         rs = stmt.getResultSet();
                           
                         while (rs.next()) {
                            timemax = rs.getInt(1);
                         }
                             
                       
                     int rowsSelected = stmt.getUpdateCount();
                     if (rowsSelected >= 0){
                           //logger.debug(rowsSelected + "  row(s) affected");
                     }
                        
                  } 
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while (hasMoreResults);
             
               attemptClose(rs);
               attemptClose(stmt);
               //powrót do mastera
               conn.setCatalog("master");
        } 
        catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError=true;
               errorDescr = e.getMessage();
       } finally {
             executeTime = System.currentTimeMillis()-executeTime;
       }
               
        return timemax;

   }
     
   public int max_time_blocked_for_ipaddr (String ipaddr,  String system_view){
                    
       String query="select -1";
       int timemax=0;
       
       
        if (system_view.equalsIgnoreCase("cluster")){
           query="select  coalesce( max(time_blocked)/60, 0) from master..sysprocesses where suid > 0 and spid != @@spid and spid!=0 and status = 'lock sleep' and blocked in (select spid from master..sysprocesses where ipaddr = '"+ipaddr+"' and instanceid=@@instanceid) at isolation 0";
        } else {
           query="select  coalesce( max(time_blocked)/60, 0) from master..sysprocesses where suid > 0 and spid != @@spid and spid!=0 and status = 'lock sleep' and blocked in (select spid from master..sysprocesses where ipaddr = '"+ipaddr+"') at isolation 0";
        }
     
        if (conn == null){
            logger.fatal("Could not create connection to database server " + query);
            isError=true;
            errorDescr = "Could not create connection to database server " + query;
            return timemax;
        }
        
        
        try {
                executeTime = System.currentTimeMillis();
                
                conn.setCatalog("master");
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                
              //  DatabaseMetaData _DatabaseMetaData = conn.getMetaData();    
                Statement stmt = conn.createStatement();
                
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                                  
                isResult = stmt.execute(query);
           
                do {
                
               
                   if (isResult) {
                     
                         rs = stmt.getResultSet();
                           
                         while (rs.next()) {
                            timemax = rs.getInt(1);
                         }
                             
                       
                     int rowsSelected = stmt.getUpdateCount();
                     if (rowsSelected >= 0){
                           //logger.debug(rowsSelected + "  row(s) affected");
                     }
                        
                  } 
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while (hasMoreResults);
             
               attemptClose(rs);
               attemptClose(stmt);
               //powrót do mastera
               conn.setCatalog("master");
        } 
        catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError=true;
               errorDescr = e.getMessage();
       } finally {
             executeTime = System.currentTimeMillis()-executeTime;
       }
               
        return timemax;

   }
    
   
    public double [] sp_spaceused_exc (String database, String table){
      
       double[] space = new double[6];
       for (int i=0; i<6;i++) space[i]=0.0;
               
       String query="select 1";
       String imageIndex= "t"+table;
      
       query="sp_spaceused " + table + ", 1";
       
        if (conn == null){
            logger.fatal("Could not create connection to database server " + query);
            isError=true;
            errorDescr = "Could not create connection to database server " + query;
            return space;
        }
        
        double index_size=0.0;
       // double index_reserved=0.0;
       // double index_unused=0.0;
        
        double text_size=0.0;
       // double text_reserved=0.0;
      //  double text_unused=0.0;
        
        double rowtotal=0.0;
        double reserved=0.0;
        double data=0.0;
        double unused=0.0;
        
        try {
                executeTime = System.currentTimeMillis();
                
                conn.setCatalog(database);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                
              //  DatabaseMetaData _DatabaseMetaData = conn.getMetaData();    
                Statement stmt = conn.createStatement();
                
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                
                int updates = 0;
                SQLWarning sqlW = null;  
                            
                isResult = stmt.execute(query);
           
                do {
                
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                          stmt.clearWarnings();
                    } 
                   if (isResult) {
                     
                         rs = stmt.getResultSet();
                         rsmd = rs.getMetaData();   
                         int numCols = rsmd.getColumnCount ();
       
                         while (rs.next()) {
                               
                                if (numCols==4) {
                                  
                                   
                                   if (rs.getString(1).equals(imageIndex)){
                                     //  System.out.println("Image");
                                       text_size+=Double.parseDouble(rs.getString(2).replace("KB", "").replace("\\s+", ""));
                                      // text_reserved+=Double.parseDouble(rs.getString(3).replace("KB", "").replace("\\s+", ""));
                                      // text_unused+=Double.parseDouble(rs.getString(4).replace("KB", "").replace("\\s+", ""));                                       
                                   } else { 
                                      // System.out.println("indeksy");
                                       index_size+=Double.parseDouble(rs.getString(2).replace("KB", "").replace("\\s+", ""));
                                      // index_reserved+=Double.parseDouble(rs.getString(3).replace("KB", "").replace("\\s+", ""));
                                       //index_unused+=Double.parseDouble(rs.getString(4).replace("KB", "").replace("\\s+", "")); 
                                   }
                                   
                                    
                                }   
                                
                                if (numCols==6) {
                                  // System.out.println("Resume");
                                           
                                   rowtotal=1.0*rs.getLong(2);
                                   reserved=Double.parseDouble(rs.getString(3).replace("KB", "").replace("\\s+", ""));
                                   data=Double.parseDouble(rs.getString(4).replace("KB", "").replace("\\s+", ""));
                                   unused=Double.parseDouble(rs.getString(6).replace("KB", "").replace("\\s+", ""));
                                                                   
                               } 
                        }
                         
                                
                       
                     int rowsSelected = stmt.getUpdateCount();
                     if (rowsSelected >= 0){
                           //logger.debug(rowsSelected + "  row(s) affected");
                     }
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {

                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while ((hasMoreResults) || (updates != -1));
             
               attemptClose(rs);
               attemptClose(stmt);
               //powrót do mastera
               conn.setCatalog("master");
        } 
        catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError=true;
               errorDescr = e.getMessage();
       } finally {
             executeTime = System.currentTimeMillis()-executeTime;
        }
        if (space[0]<0) {
            isError=true; 
            errorDescr = "[E0001] Problem with column counter";
        }
        
        space[0]=rowtotal;
        space[1]=reserved/1024.0;
        space[2]=data/1024.0;
        space[3]=index_size/1024.0;
        space[4]=text_size/1024.0;
        space[5]=unused/1024.0;
        
        //for (int i=0; i<6;i++) System.out.println(space[i]);
        
        return space;

   }
    
    
 
 private double dispSpace_exc (ResultSet rs, boolean prc, boolean free) {
  
     double space=0.0;

     try {
         ResultSetMetaData rsmd = rs.getMetaData ();
     

          // Get the number of columns in the result set

          int numCols = rsmd.getColumnCount ();
          if (numCols<5) {
              isError=true;
              errorDescr="[E0002] numCols<5";
              return -2.0;
          }


          String TmpTotalSize = rs.getString(1);
          String TotalSize = TmpTotalSize.replace("MB", "0");

          long TotalPages = rs.getLong(2);
          long FreePages = rs.getLong(3);
          long UsedPages = rs.getLong(4);
        //  int ReservedPages = rs.getInt(5);

          if (free){
            if (prc){
                  double FreeSpace_prc = (1.0*FreePages)/(1.0*TotalPages) * 100.0;
                  space=FreeSpace_prc ;
              } else {
                  double FreeSpace = (1.0*FreePages)/(1.0*TotalPages) * Double.parseDouble(TotalSize);
                  space = FreeSpace;
             }
          } else {
              if (prc){
                  double UsedSpace_prc = (1.0*UsedPages)/(1.0*TotalPages) * 100.0;
                  space=UsedSpace_prc ;
              } else {
                  double UsedSpace = (1.0*UsedPages)/(1.0*TotalPages) * Double.parseDouble(TotalSize);
                  space = UsedSpace;
             }
          }
         } catch (Exception e){
             logger.error(e);
         }

      return space;
  }
 
 private double[] dispSpace_AllInOne (ResultSet rs)  throws SQLException {
  
      double[] space = new double[4];
      space[0] =0.0;
      space[1] =0.0;
      space[2] =0.0;
      space[3] =0.0;
        
      ResultSetMetaData rsmd = rs.getMetaData ();

      // Get the number of columns in the result set

      int numCols = rsmd.getColumnCount ();
      if (numCols<5) {
          isError=true;
          errorDescr="[E0002] numCols<5";
          space[0] =-2.0;
          space[1] =-2.0;
          space[2] =-2.0;
          space[3] =-2.0;
          return space;
      }

             
      String TmpTotalSize = rs.getString(1);
      String TotalSize = TmpTotalSize.replace("MB", "");
      
      long TotalPages = 1;
      long FreePages = 0;
      long UsedPages = 0;
      
      try {
          TotalPages = Long.parseLong(rs.getString(2).replaceAll("\\s+", ""));
      } catch (Exception e){
          logger.error("E21" + e);
      }
      try {
           FreePages = Long.parseLong(rs.getString(3).replaceAll("\\s+", ""));
      } catch (Exception e){
          logger.error("E22" +e);
      }
      try {
           UsedPages = Long.parseLong(rs.getString(4).replaceAll("\\s+", ""));
      } catch (Exception e){
          logger.error("E23" +e);
      }
    //  int ReservedPages = rs.getInt(5);

     //free
     double FreeSpace = (1.0*FreePages)/(1.0*TotalPages) * Double.parseDouble(TotalSize);
     space[0] = FreeSpace;
      
     double FreeSpace_prc = (1.0*FreePages)/(1.0*TotalPages) * 100.0;
     space[1] = FreeSpace_prc ;
        
        
     //used
     double UsedSpace = (1.0*UsedPages)/(1.0*TotalPages) * Double.parseDouble(TotalSize);
     space[2] =  UsedSpace;
              
     double UsedSpace_prc = (1.0*UsedPages)/(1.0*TotalPages) * 100.0;
     space[3] = UsedSpace_prc ;
        
     return space;
  }
 
 
 
  private String convertSpaceFromMegaBytes(double space, String unit){
   
         if (space==0){
             return "0";
         }  
         
        if (unit.equals("bit")){ 
            space = space*1024.0*1024.0*8.0;
            return returnString(space); 
        }
         
         
        if (unit.equalsIgnoreCase("B")){ 
            space = space*1024.0*1024.0;
            return returnString(space); 
        }
         
        if (unit.equalsIgnoreCase("K")){ 
            space = space*1024.0;
            return returnString(space); 
        }
        
        if (unit.equalsIgnoreCase("M")){ 
            return returnString(space); //bez zmian
        }
        
        if (unit.equalsIgnoreCase("G")){ 
            space = space/1024.0;
            return returnString(space); 
        }
        
        if (unit.equalsIgnoreCase("T")){ 
            space = space/(1024.0*1024.0);
            return returnString(space); 
        } 
        
        if (unit.equalsIgnoreCase("P")){ 
            space = space/(1024.0*1024.0*1024.0);
            return returnString(space); 
        }  
        
        return "error";
    }

       
     protected boolean checkForTSQLPrint(SQLException ex)  {
         boolean returnVal = false;
         if ((ex.getErrorCode() == 0) && (ex.getSQLState() == null))
         {
           returnVal = true;

         }
         return returnVal;
     }
        
    protected String printWarnings(SQLException ex){
         StringBuilder retString=new StringBuilder();

         while (ex != null)
         {
           if (checkForTSQLPrint(ex))
           {
               retString.append(ex.getMessage());
               retString.append(newline);
               ex = ex.getNextException();
           } else {
               retString.append(ex.getMessage());
               retString.append(newline);
               ex = ex.getNextException();
           }
         }

         return retString.toString();
             
    }
    
    private int getSybaseEngineCount(Connection conn){
        
        if (conn == null){
            logger.error("connection is null getSybaseEngineCount");
            isError=true;
            errorDescr =  "connection is null getSybaseEngineCount";
            return -200;
        }
        
        //String newline = System.getProperty("line.separator");
              
        int total;
        total=0;
                             
        String query = "select engine, status from master..sysengines"; 
            
        try {
                executeTime = System.currentTimeMillis();
                
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                     
                while (rs.next())  {
                        int engine = rs.getInt(1); 
                        String status  = rs.getString(2);
                        total++;
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
               isError=true;
               errorDescr = e.getMessage();
        } finally{
          executeTime = System.currentTimeMillis()-executeTime;
        }
        
        return total;
    }
  
     
    
    
   private String doNumericQuery(){
        
        String ret="-299";
      
        
        if (conn == null){
            logger.error("Could not create connection to database server doNumericQuery");
            isError=true;
            errorDescr = "Could not create connection to database server doNumericQuery";
            return "-200";
        }
        
      
        try {
                executeTime = System.currentTimeMillis();
                
                if (sybaseApiList.get(__queue).getIsolationLevel()==-1) {
                    //conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                } else {
                    conn.setTransactionIsolation(sybaseApiList.get(__queue).getIsolationLevel());
                }
                        
                if (!sybaseApiList.get(__queue).getDatabaseProperty("use_database", "no_use").equalsIgnoreCase("no_use")){
                    conn.setCatalog(sybaseApiList.get(__queue).getDatabaseProperty("use_database", "no_use"));
                }
           
               
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                 
                int updates = 0;
                SQLWarning sqlW = null;  
                             
                int more_stmt_count = 1;
                
                isResult = stmt.execute(sybaseApiList.get(__queue).getSqlQuery());
                if (logger.isDebugEnabled()) logger.debug("Execute query " + sybaseApiList.get(__queue).getSqlQuery());
                
                do {
                  
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                          if (sybaseApiList.get(__queue).getMoreStmt()==more_stmt_count) ret = "-301";
                          stmt.clearWarnings();
                    } 
                 
                   if (isResult) {
                     
                          rs = stmt.getResultSet();
                          rsmd = rs.getMetaData();
                          
                          int column = sybaseApiList.get(__queue).getColumn();

                          int numCol = rsmd.getColumnCount();
                          if (column > numCol){
                                logger.warn("**** col > colcount ****");
                                column = 1;
                         }

                         int currrent_row=1;         
                         while (rs.next()) {
                                if (sybaseApiList.get(__queue).getMoreStmt()==more_stmt_count) {
                                    
                                    String numberRefined = rs.getString(column);
                                    if (rs.wasNull()) {
                                        numberRefined = "0";
                                    } else {
                                    }
                                    if (sybaseApiList.get(__queue).isInteger()){
                                        ret = numberRefined.replaceAll("[^0-9]", "");
                                    } else if (sybaseApiList.get(__queue).isLong()){
                                        numberRefined=numberRefined.replaceAll("[^0-9]", "");
                                        ret = numberRefined;
                                    } else if (sybaseApiList.get(__queue).isFloat()){
                                         numberRefined=numberRefined.replaceAll("[^0-9,.]", "");
                                         numberRefined=numberRefined.replace(",", ".");
                                         ret = numberRefined;
                                    }
                                
                                }

                                if (sybaseApiList.get(__queue).getRow()== currrent_row){
                                   break;
                                }
                                currrent_row++;
                         }
                
                        more_stmt_count++;
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         //logger.warn(sqlW);
                         stmt.clearWarnings();
                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while ((hasMoreResults) || (updates != -1));
             
                attemptClose(rs);
                attemptClose(stmt);
        } 
        catch (SQLException e) {
               ret="-205";
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError=true;
               errorDescr = e.getMessage();
       } finally {
              executeTime = System.currentTimeMillis()-executeTime;
        }
        
        return ret;

     } 
  
   private String getQueryHelp(String q){
      
          
        if (conn == null){
            logger.error("Could not create connection to database server getQueryHelp");
            isError=true;
            errorDescr = "Could not create connection to database server getQueryHelp";
            return "CONN_ERROR";
        }
        
        String ret="default";
     
        try {
                executeTime = System.currentTimeMillis();
            
               
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY); 
                
                            
                ResultSet rs = null;
                boolean isResult = false;
              
                
                isResult = stmt.execute(q);
              
                   if (isResult) {
                     
                         rs = stmt.getResultSet();

      
                         while (rs.next()) {
                              ret = rs.getString(1);
                         }
                  
                        
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
               ret = "ERROR_SQL";
               isError=true;
               errorDescr = e.getMessage();
       } finally {
             executeTime = System.currentTimeMillis()-executeTime;
       }
       
       return ret;

   }
    
    private String doStringQuery(){
      
          
        if (conn == null){
            logger.error("Could not create connection to database server doStringQuery");
            isError=true;
            errorDescr = "Could not create connection to database server doStringQuery";
            return "CONN_ERROR";
        }
        
        String ret="default";
     
        try {
                executeTime = System.currentTimeMillis();
               
                if (sybaseApiList.get(__queue).getIsolationLevel()==-1) {
                   // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
                } else {
                     conn.setTransactionIsolation(sybaseApiList.get(__queue).getIsolationLevel());
                }  
            
                if (!sybaseApiList.get(__queue).getDatabaseProperty("use_database", "no_use").equalsIgnoreCase("no_use")){
                    conn.setCatalog(sybaseApiList.get(__queue).getDatabaseProperty("use_database", "no_use"));
                }
           
               
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY); 
                
                            
                ResultSet rs = null;
                ResultSetMetaData rsmd = null;
                boolean isResult = false;
                boolean hasMoreResults = false;
                 
                int updates = 0;
                SQLWarning sqlW = null;  
           
                int more_stmt_count = 1;
                
                isResult = stmt.execute(sybaseApiList.get(__queue).getSqlQuery());
                         
                do {
                  
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 

                          if (sybaseApiList.get(__queue).getMoreStmt()==more_stmt_count) ret = printWarnings(sqlW);
                          stmt.clearWarnings();
                    } 
                 
                   if (isResult) {
                         int column = sybaseApiList.get(__queue).getColumn();
                     
                         rs = stmt.getResultSet();
                         rsmd = rs.getMetaData();


                          int numCol = rsmd.getColumnCount();
                          if (column > numCol){
                                logger.warn("**** col > colcount ****");
                                column = 1;
                         }

                         int currrent_row=1;         
                         while (rs.next()) {
                                if (sybaseApiList.get(__queue).getMoreStmt()==more_stmt_count) {
                                    ret = rs.getString(column);
                                    if (rs.wasNull()) {
                                        ret = "0";
                                    } else {
                                    }
                                }

                                if (sybaseApiList.get(__queue).getRow() == currrent_row){
                                   break;
                                }
                                currrent_row++;
                         }
                  
                     more_stmt_count++;
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         stmt.clearWarnings();
                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;                   
                }  while ((hasMoreResults) || (updates != -1));
             
               attemptClose(rs);
               attemptClose(stmt);
        } 
        catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               ret = "ERROR_SQL";
               isError=true;
               errorDescr = e.getMessage();
       } finally {
             executeTime = System.currentTimeMillis()-executeTime;
       }
       
       return ret;

   }
    
    private String doQuery(){
            
       
            boolean _isError = false;
            boolean _isWarn = false;
            String ret="default";
             
            StringBuilder out = new StringBuilder();
     

            if (conn == null){
                logger.error("Could not create connection to database server doQuery");
                isError=true;
                errorDescr = "Could not create connection to database server doQuery";
                return "CONN_ERROR";
            }
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
            
             try {  
                executeTime = System.currentTimeMillis();
                
                if (sybaseApiList.get(__queue).getIsolationLevel()==-1) {
                   // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
                } else {
                     conn.setTransactionIsolation(sybaseApiList.get(__queue).getIsolationLevel());
                }
                
                if (!sybaseApiList.get(__queue).getDatabaseProperty("use_database", "no_use").equalsIgnoreCase("no_use")){
                    conn.setCatalog(sybaseApiList.get(__queue).getDatabaseProperty("use_database", "no_use"));
                }
                
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
              
              
               isResult = stmt.execute(sybaseApiList.get(__queue).getSqlQuery());
          
               int updates = 0;
               SQLWarning sqlW = null;  
               sqlW = stmt.getWarnings();
                 
               do {
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                        if (!sybaseApiList.get(__queue).isNoData()) out.append(printWarnings(sqlW));
                        _isWarn = true;
                        stmt.clearWarnings();
                    } 
                 
                  if (isResult) {
                 
                      rs = stmt.getResultSet();
                      rsmd = rs.getMetaData();
                    
                      int numCol = rsmd.getColumnCount();
                      if (!sybaseApiList.get(__queue).isNoData()){ 
                          if (sybaseApiList.get(__queue).isHtml()) out.append("<table><tr>");
                          for (int i=1; i<=numCol; i++) {
                               if (sybaseApiList.get(__queue).isHtml()){
                                    out.append("<th>");
                                    out.append(rsmd.getColumnName(i));
                                    out.append("</th>");
                               } else {
                                 out.append(rsmd.getColumnName(i));
                                 out.append(" | ");
                               }
                          }  
                          if (sybaseApiList.get(__queue).isHtml()) {
                              out.append("</tr>");
                          } else {
                              out.append(newline);
                          }
                               
                      }
                                       
                     while (rs.next()) {
                           if (!sybaseApiList.get(__queue).isNoData()) {
                               if (sybaseApiList.get(__queue).isHtml()) out.append("<tr>");
                           }
                           for (int i=1; i<=numCol; i++) {
                                if (!sybaseApiList.get(__queue).isNoData()){
                                    String add = rs.getString(i);
                                    if (rs.wasNull()) {
                                        add = "(NULL)";
                                    } else {
                                    }
                                    if (sybaseApiList.get(__queue).isHtml()){
                                        out.append("<td>");
                                        out.append(add);
                                        out.append("</td>");
                                    } else {
                                         out.append(add);
                                        out.append(" | ");
                                    }
                                    
                                }
                                 
                           }
                           if (!sybaseApiList.get(__queue).isNoData()) {
                                if (sybaseApiList.get(__queue).isHtml()) {
                                    out.append("</tr>");
                                } else {
                                    out.append(newline);
                                }
                           }
                      }
                              
                    if (!sybaseApiList.get(__queue).isNoData()){ 
                          if (sybaseApiList.get(__queue).isHtml()) out.append("</table>"); 
                    }
                    
                    int rowsSelected = stmt.getUpdateCount();
                    if (rowsSelected >= 0){
                        if (!sybaseApiList.get(__queue).isNoData()) {
                            if (sybaseApiList.get(__queue).isHtml()) out.append("<br/>"); 
                            if (sybaseApiList.get(__queue).isShowCount()) out.append(rowsSelected).append("  row(s) affected");
                            if (sybaseApiList.get(__queue).isHtml()) out.append("<br/>");
                            else out.append(newline);
                        }
                    }
                     
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         logger.warn(sqlW);
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {
                         if (!sybaseApiList.get(__queue).isNoData()) {
                            if (sybaseApiList.get(__queue).isHtml()) out.append("<br/>"); 
                          
                            if (sybaseApiList.get(__queue).isShowCount())  out.append(updates + " row(s) updated");
                          
                            if (sybaseApiList.get(__queue).isHtml()) out.append("<br/>");
                            else out.append(newline);
                         };

                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while ((hasMoreResults) || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);

            }  catch (SQLException e) {             
                    if (!sybaseApiList.get(__queue).isNoData()) {
                          if (sybaseApiList.get(__queue).isHtml()){
                             out.append( e.getMessage());
                             out.append("<br/>");
                          }  else{
                             out.append( e.getMessage());
                             out.append(newline);
                          }
                    }
                     
                    logger.error("SQL Exception/Error for query " + sybaseApiList.get(__queue).getSqlQuery());
                    
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
                    
                    isError=true;
                    errorDescr = e.getMessage();
                                       
                    
                    _isError=true;
             } finally {
                  executeTime = System.currentTimeMillis()-executeTime;
             }
             
             if (sybaseApiList.get(__queue).isNoData()) {
                 if (_isWarn) ret ="WARN";
                 if (_isError) ret ="ERROR";
                 if (!_isError && !_isWarn) ret ="EXECUTED";
             } else ret=out.toString();
          
             return ret;
             
      }
    
     private String doQueryCompare(){
            
        Connection conn1 = getConnectionRetry("single", sybaseApiList.get(__queue).getDatabaseProperty("sybase_host_master","master"),
                Integer.parseInt(sybaseApiList.get(__queue).getDatabaseProperty("sybase_port_master","25")),
                sybaseApiList.get(__queue).getDatabaseProperty("sybase_user_master","master"),
                sybaseApiList.get(__queue).getDatabaseProperty("sybase_password_master","master"), 
                Integer.parseInt(sybaseApiList.get(__queue).getDatabaseProperty("sybase_timeout_master","3")), 
                Integer.parseInt(sybaseApiList.get(__queue).getDatabaseProperty("sybase_retry_master","2")));
                
        Connection conn2 = getConnectionRetry("single", sybaseApiList.get(__queue).getDatabaseProperty("sybase_host_slave","slave"),
                Integer.parseInt(sybaseApiList.get(__queue).getDatabaseProperty("sybase_port_slave","25")),
                sybaseApiList.get(__queue).getDatabaseProperty("sybase_user_slave","slave"),
                sybaseApiList.get(__queue).getDatabaseProperty("sybase_password_slave","slave"), 
                Integer.parseInt(sybaseApiList.get(__queue).getDatabaseProperty("sybase_timeout_slave","3")), 
                Integer.parseInt(sybaseApiList.get(__queue).getDatabaseProperty("sybase_retry_slave","2")) );
        
        if (conn1 == null || conn2 == null){
            logger.error("Connection 1 or 2 is null");
            isError = true; 
            errorDescr = "Connection 1 or 2 is null";
            return "ERROR_CONN";
        }
        
        String ret="default";
        String ret1="default";
        String ret2="default";
     
        try {
                if (sybaseApiList.get(__queue).getDatabaseProperty("isolation_level", "default").equalsIgnoreCase("default")) {
                
                } else {
                   conn1.setTransactionIsolation(sybaseApiList.get(__queue).getIsolationLevel());
                   conn2.setTransactionIsolation(sybaseApiList.get(__queue).getIsolationLevel());
                }
               
                Statement stmt1 = conn1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                Statement stmt2 = conn2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                
                ResultSet rs1 = null;
                ResultSet rs2 = null;
                ResultSetMetaData rsmd1 = null;
                ResultSetMetaData rsmd2 = null;
                boolean isResult1 = false;
                boolean isResult2 = false;
               
                String query = sybaseApiList.get(__queue).getSqlQuery();
                
                isResult1 = stmt1.execute(sybaseApiList.get(__queue).getDatabaseProperty("sql-query_master", query));
                isResult2 = stmt2.execute(sybaseApiList.get(__queue).getDatabaseProperty("sql-query_slave", query));
                               
                int ile_ok=0, ile_bad=0;
                             
                if (isResult1 && isResult2) {
                     
                          rs1 = stmt1.getResultSet();
                          rs2 = stmt2.getResultSet();
                          rsmd1 = rs1.getMetaData();
                          rsmd2 = rs2.getMetaData();

                          int numCol1 = rsmd1.getColumnCount();
                          int numCol2 = rsmd2.getColumnCount();
                          
                          if (numCol1!=numCol2) {
                              isError = true;
                              return "numCol1<>numCol2";
                          }
                        
                          
                          while (rs1.next()) {
                              if (rs2.next()){
                                     for (int i=1; i<= numCol1; i++){
                                        ret1 = rs1.getString(i);
                                        ret2 = rs2.getString(i);
                                        //logger.debug(ret1 + " " + ret2);
                                        if (ret1==null && ret2==null){
                                            ile_ok++;
                                        }
                                        else if (ret1==null && ret2!=null){
                                            ile_bad++;
                                            logger.debug("COMPARE BAD " + ret1 + "<>" + ret2);
                                        }
                                        else if (ret1!=null && ret2==null){
                                            ile_bad++;
                                            logger.debug("COMPARE BAD " + ret1 + "<>" + ret2);
                                        }
                                        else if (ret1!=null && ret2!=null) {
                                            if (ret1.equalsIgnoreCase(ret2)){
                                                 ile_ok++;
                                            } else {
                                                ile_bad++;
                                            }
                                        }
                                        else {
                                            ile_bad++;
                                            logger.debug("COMPARE ELSE BAD " + ret1 + "<>" + ret2);
                                        }
                                     }
                                
                              } else {
                                 return "numRow1<>numRow2"; 
                              }
                           
                          }
                          double res=(100.0*ile_ok)/(1.0*(ile_ok+ile_bad));
                          ret = "" + res; 
                     
                  } else   {
                      isError=true;
                      return "No results";
                  }
             
                attemptClose(rs1);
                attemptClose(rs2);
                attemptClose(stmt1);
                attemptClose(stmt2); 
                attemptClose(conn1);
                attemptClose(conn2);
              
        
        } catch (SQLException e) { 
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               ret = "ERROR_SQL"; 
               isError = true; 
               errorDescr = e.getMessage();
       }
    
        return ret;

    }
     
     private int killPIDs(){
       
            
            if (conn==null) {
                isError=true;
                return -200;
            }
            
            String kill_query = sybaseApiList.get(__queue).getDatabaseProperty("kill-query", "SELECT -100"); 
           
            List<String> pids =  getPIDtoKill(conn,  kill_query);
            int ilekill=0;

            ilekill = pids.stream().filter((pid) -> ( killPID(conn, pid) )).map((_item) -> 1).reduce(ilekill, Integer::sum);
            
            pids.clear();
          
            return ilekill;
          
    }
    
    
    private boolean killPID(Connection conn, String pid){
              
         if (conn == null){
            logger.error("Could not create connection to database server killPID");
            isError=true;
            return false;
         }
      
         try {
                conn.setCatalog("master");
                
                CallableStatement _cstmt = conn.prepareCall("{call kill " + pid + "}");
                boolean results = _cstmt.execute();
                int rowsAffected = 0; 
                do  {
                    if(results) {
                        try (ResultSet rs = _cstmt.getResultSet()) {
                            
                        }
                    }   else  {
                        rowsAffected = _cstmt.getUpdateCount();
                        
                    }
                        results = _cstmt.getMoreResults();
                } while (results || rowsAffected != -1);
                
                attemptClose(_cstmt);
          
       } 
        catch (SQLException e) {
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               return false;
        }
       
        return true;
      
    }
  
   public List<List<String>> getDiscoveryMap(int _queue, String __query){
      
        this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false; 
           
        conn = getConnectionRetry();  
        if (conn==null){
            isError = true;
            return null;
        } 
     
        List<List<String>> values = new ArrayList<>();
        ResultSet rs = null;
        boolean isResult = false;
        boolean hasMoreResults = false;
         
        try { 
            
           executeTime = System.currentTimeMillis();
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           isResult = stmt.execute(__query);
           logger.trace("getDiscoveryMap " + __query);

           int updates = 0;
           boolean add_metadata=true;
                 
           do {
                 if (isResult) {
                 
                   rs = stmt.getResultSet();         
                   
                   if (add_metadata){
                       List<String> metaData = new ArrayList<>();

                       ResultSetMetaData meta = rs.getMetaData();
                       for (int i = 1; i <= meta.getColumnCount(); i++) {
                           metaData.add(meta.getColumnLabel(i).toUpperCase());
                       }
                       values.add(metaData);
                       add_metadata=false;
                   }
                                                           
                   while (rs.next()) {
                           List<String> Data = new ArrayList<>();
                           for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                                  String dt = rs.getString(i);
                                  if (dt!=null){
                                     Data.add(dt.replace("\\", "\\\\"));
                                  } else {
                                      Data.add("null");
                                  }
                           }
                           values.add(Data);
                           logger.trace("returned " + Data.toString());
                      }
                    
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                    
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while ((hasMoreResults) || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);

            
         }   catch (SQLException e) {
               isError=true;
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               logger.error(__query);
               logger.error(sybaseApiList.get(_queue).getSybaseInterface());
        } finally {
             executeTime = System.currentTimeMillis()-executeTime;
        }
        //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single")) 
        attemptClose(conn);
        
        return values;
    } 
   
    public List<List<String>> getRepAllQueuesMap(int _queue){
      
        this.__queue=_queue;     
        String database = sybaseApiList.get(__queue).getDatabaseProperty("database", "rssd"); 
        List<List<String>> values = new ArrayList<>();
           
        logger.debug(database);        
        
        StringBuilder query1 = new StringBuilder();
         
        query1.append("select distinct dsname + '.' + dbname as Polaczenie,  'Inbound' as Typ_kolejki,  0 as Kolejka, number, q.state, q.type as Typ_kolejki_sym")
                .append(" from rs_queues q, rs_databases d")
                .append(" where number = d.dbid and type=1"); 

         
        query1.append(" select distinct  isnull(convert(varchar(61), name), dsname+'.'+dbname), 'Outbound' as Typ_kolejki, 0 as Kolejka, number, q.state, q.type as Typ_kolejki_sym")	 
         .append(" from rs_queues q, rs_databases, rs_sites") 		 
         .append(" where number *= dbid")			 
         .append(" and number *= id and type=0");
         
         List<String> metaData = new ArrayList<>();
         metaData.add("QUEUE");
         metaData.add("TYPE");
         values.add(metaData);
                  
         boolean hasMoreResults = false;
         boolean isResult = false;
         ResultSet rs = null;
         ResultSetMetaData rsmd = null;
         
        this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false; 
             
                 
        conn = getConnectionRetry();  
        
        if (conn==null){
            isError = true;
            return null;
        }
           
         try {
           executeTime = System.currentTimeMillis();
           conn.setCatalog(database);
           
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           isResult = stmt.execute(query1.toString());
           int updates = 0;


           do {
             if (isResult){
                rs = stmt.getResultSet();
                rsmd = rs.getMetaData();
                
                int numCols = rsmd.getColumnCount ();             
                
                while (rs.next())  {
                     List<String> row = new ArrayList<>();
                     String name = rs.getString(1); 
                     if (rs.wasNull()) {
                        continue;  
                     }
                   
                     String type = rs.getString(2);
                     
                     if (rs.wasNull()) {
                        continue;  
                     }
                   
                     if (name!=null && type!=null){
                        row.add(name);
                        row.add(type);
                     }  
                      
                      values.add(row);
                     
                }
                int rowsSelected = stmt.getUpdateCount();
                if (rowsSelected >= 0){
                    //  if (logger.isDebugEnabled()) logger.debug(rowsSelected + " ROWS_AFFECTED");
                }
            } //end if  
            
            else {
               updates = stmt.getUpdateCount();
            }
             hasMoreResults = stmt.getMoreResults();
             isResult = hasMoreResults;
           } //end do
           while ((hasMoreResults) || (updates != -1));
           
           attemptClose(rs);
           attemptClose(stmt);
        
        }   
        
       
        catch (SQLException e) {
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError = true;
               errorDescr = e.getMessage();
        } finally {
              executeTime = System.currentTimeMillis()-executeTime;
        }
      
         List<String> row1 = new ArrayList<>();
        List<String> row2 = new ArrayList<>();
        List<String> row3 = new ArrayList<>();
        
        row1.add("Total");
        row1.add("Inbound_Outbound");          
        values.add(row1);
        
        row2.add("Total");
        row2.add("Inbound");  
        values.add(row2);
        
        row3.add("Total");
        row3.add("Outbound"); 
        values.add(row3);
       
        //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single")) 
        attemptClose(conn);
        return values;
       
   }
    
    public List<List<String>> getSybaseThreadAndEngine(int _queue){
                
        String query ="select 'Thread ' + CONVERT(varchar(4), ThreadID) as THREAD , 'Engine ' + CONVERT(varchar(4), EngineNumber) as ENGINE from master..monEngine where instanceID=@@instanceID  and EngineNumber in (select engine from master..sysengines where status='online' and instanceID=@@instanceID)\n" +
            "union\n" +
            "select distinct  'Thread ' +  CONVERT(varchar(4), ThreadID) as THREAD,  name as ENGINE from master..monTask where instanceID=@@instanceID and name in ('CIPC Controller', 'DiskController', 'IP Link Monitor', 'NetController',  'Signal Handler') group by instanceid, name"; 
 
        return  getDiscoveryMap(_queue, query);
        
       
   }
    
    
    public List<List<String>> getSybaseThread(int _queue){
    
        String query ="select 'Thread ' + CONVERT(varchar(4), ThreadID) as THREAD from master.dbo.monEngine where instanceID=@@instanceID  and EngineNumber in (select engine from master..sysengines where status='online')\n" +
            "  union\n" +
            "select distinct  'Thread ' +  CONVERT(varchar(4), ThreadID) as THREAD from master.dbo.monTask where instanceID=@@instanceID and name in ('CIPC Controller', 'DiskController', 'IP Link Monitor', 'NetController',  'Signal Handler') group by instanceid, name"; 
    
        return  getDiscoveryMap(_queue, query);
       
   }
     
   
   public List<List<String>> getRepAllThreadsMap(int _queue){
        
       String query ="admin who, no_trunc"; 
       this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false; 
             
                 
        conn = getConnectionRetry();  
        if (conn==null){
            isError = true;
            return null;
        }
        
        List<List<String>> values = new ArrayList<>();
        ResultSet rs = null;
         
        try {
           executeTime = System.currentTimeMillis();
           
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           rs = stmt.executeQuery(query);
                  
	   List<String> metaData = new ArrayList<>();

           ResultSetMetaData meta = rs.getMetaData();
     
           
           metaData.add("TYPE"); //add only
           metaData.add("NAME"); //add only 
           
           values.add(metaData);
         
           if (meta.getColumnCount()<4){
               isError = true;
               return null;
           }

           while (rs.next()) {
                   List<String> Data = new ArrayList<>();
                   String name = rs.getString(4).replace("\\", "\\\\");
                   String type = rs.getString(2).replace("\\", "\\\\");
                   
                   if (name!=null && type!=null){
                        name = name.replaceAll("\\s+", " ");
                        type = type.replaceAll("\\s+", " ");
                        
                        if (type.charAt(type.length()-1)==' '){
                            type = type.substring(0, type.length()-1);
                        }  
                        
                        if (name.charAt(name.length()-1)==' '){
                            name = name.substring(0, name.length()-1);
                        } 
                        
                        if (type.equalsIgnoreCase("USER")) continue;
                      
                        if (name.length()<=1) continue;
                        
                        Data.add(type); 
                        Data.add(name);
                    }       
                                  
                   values.add(Data);
           }
           
             attemptClose(rs);
             attemptClose(stmt);   
        }   
     
        catch (SQLException e) {
               isError=true;
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
        } finally {
           executeTime = System.currentTimeMillis()-executeTime;
        }
             
        
        
        //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single")) 
        attemptClose(conn);
        
        return values;
    }
   
   public List<List<String>> getRepLiteThreadsMap(int _queue){
        
       String query ="admin who, no_trunc"; 
       this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false;      
                 
        conn = getConnectionRetry();  
        if (conn==null){
            isError = true;
            return null;
        }
        
        List<List<String>> values = new ArrayList<List<String>>();
        ResultSet rs = null;
         
        try {
           executeTime = System.currentTimeMillis();
           
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           rs = stmt.executeQuery(query);
                  
	   List<String> metaData = new ArrayList<String>();

           ResultSetMetaData meta = rs.getMetaData();
     
           
           metaData.add("TYPE"); //add only
           metaData.add("NAME"); //add only 
           
           values.add(metaData);
         
           if (meta.getColumnCount()<4){
               isError = true;
               return null;
           }

           while (rs.next()) {
                   List<String> Data = new ArrayList<>();
                   String name = rs.getString(4).replace("\\", "\\\\");
                   String type = rs.getString(2).replace("\\", "\\\\");
                   
                   if (name!=null && type!=null){
                        name = name.replaceAll("\\s+", " ");
                        type = type.replaceAll("\\s+", " ");
                        
                        if (type.charAt(type.length()-1)==' '){
                            type = type.substring(0, type.length()-1);
                        }  
                        
                        if (name.charAt(name.length()-1)==' '){
                            name = name.substring(0, name.length()-1);
                        } 
                        
                        if (type.equalsIgnoreCase("USER") || type.equalsIgnoreCase("SQM") || type.equalsIgnoreCase("SQT")
                                || type.equalsIgnoreCase("dSUB") || type.equalsIgnoreCase("dCM") || type.equalsIgnoreCase("dAIO")
                                || type.equalsIgnoreCase("dREC") || type.equalsIgnoreCase("dALARM")|| type.equalsIgnoreCase("dSYSAM")) continue;
                      
                        if (name.length()<=1) continue;
                        
                        Data.add(type); 
                        Data.add(name);
                    }       
                                  
                   values.add(Data);
           }
           
             attemptClose(rs);
             attemptClose(stmt);   
        }   
     
        catch (SQLException e) {
               isError=true;
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
        } finally {
          executeTime = System.currentTimeMillis()-executeTime;
        }
             
        
        
       // if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single"))
        attemptClose(conn);
        
        return values;
    }
   
  
   public List<List<String>> getRepAllQueuesValues(int _queue){
        this.__queue=_queue;
      
        String database = sybaseApiList.get(__queue).getDatabaseProperty("database", "rssd"); 
        List<List<String>> values = new ArrayList<>();
           
                  
        StringBuilder query1 = new StringBuilder();
         
        query1.append("select distinct dsname + '.' + dbname as Polaczenie,  'Inbound' as Typ_kolejki,  0 as Kolejka, number, q.state, q.type as Typ_kolejki_sym")
                .append(" from rs_queues q, rs_databases d")
                .append(" where number = d.dbid and type=1"); 

         
        query1.append(" select distinct  isnull(convert(varchar(61), name), dsname+'.'+dbname), 'Outbound' as Typ_kolejki, 0 as Kolejka, number, q.state, q.type as Typ_kolejki_sym")	 
         .append(" from rs_queues q, rs_databases, rs_sites") 		 
         .append(" where number *= dbid")			 
         .append(" and number *= id and type=0");
         
         List<String> metaData = new ArrayList<String>();
         metaData.add("QUEUE");
         metaData.add("TYPE");
         metaData.add("VALUE");
         values.add(metaData);
                  
         boolean hasMoreResults = false;
         boolean isResult = false;
         ResultSet rs = null;
         ResultSetMetaData rsmd = null;
         
        this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false;      
                 
        conn = getConnectionRetry();  
        
        if (conn==null){
            isError = true;
            return null;
        }
           
         try {
           executeTime = System.currentTimeMillis();
           conn.setCatalog(database);
           
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           isResult = stmt.execute(query1.toString());
           int updates = 0;


           do {
             if (isResult){
                rs = stmt.getResultSet();
                rsmd = rs.getMetaData();
                
                int numCols = rsmd.getColumnCount ();             
                
                while (rs.next())  {
                      List<String> row = new ArrayList<>();
                      for (int i=1; i<=numCols; i++) {
                          String tmp = rs.getString(i);
                          if (rs.wasNull()) {
                             tmp="0";
                          } else {
                          }
                          row.add(tmp);
                          //results[i][j]=tmp;
                      }
                      values.add(row);
                      //j++;
                }
                int rowsSelected = stmt.getUpdateCount();
                if (rowsSelected >= 0){
                    //  if (logger.isDebugEnabled()) logger.debug(rowsSelected + " ROWS_AFFECTED");
                }
            } //end if  
            
            else {
               updates = stmt.getUpdateCount();
            }
             hasMoreResults = stmt.getMoreResults();
             isResult = hasMoreResults;
           } //end do
           while ((hasMoreResults) || (updates != -1));
           
           attemptClose(rs);
           attemptClose(stmt);
        
        }   
        
       
        catch (SQLException e) {
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
               isError = true;
               errorDescr = e.getMessage();
        } finally {
            executeTime = System.currentTimeMillis()-executeTime;
         }
         
        int total=0;
        int inbound=0;
        int outbound=0;
        String _type, _value;
        int val=0;
         
        for (int j=1;j<values.size();j++){ 
             
               values.get(j).set(2, rs_kol_helper(conn, values.get(j).get(3), values.get(j).get(5)));
             
              _type = values.get(j).get(1);
              _value = values.get(j).get(2);
              
              try {
                   val = Integer.parseInt(_value);
               } catch(NumberFormatException ir) {
                   val=0;
               }
              
              total+=val;
       
              if (_type.equalsIgnoreCase("inbound"))   inbound+=val;
              if (_type.equalsIgnoreCase("outbound"))  outbound+=val;
              
              //clean
              values.get(j).remove(5);
              values.get(j).remove(4);
              values.get(j).remove(3);
             
        }
        
        //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single"))
        attemptClose(conn);
        
        List<String> row1 = new ArrayList<>();
        List<String> row2 = new ArrayList<>();
        List<String> row3 = new ArrayList<>();
        
        row1.add("Total");
        row1.add("Inbound_Outbound");    
        row1.add(""+total);        
        values.add(row1);
        
        row2.add("Total");
        row2.add("Inbound");   
        row2.add(""+inbound);
        values.add(row2);
    
        row3.add("Total");
        row3.add("Outbound");   
        row3.add(""+outbound);
        values.add(row3);
        
        return values;
       
   }
   
   public List<List<String>> getRepAllThreadsValue(int _queue){
        
       String query ="admin who, no_trunc"; 
       this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false;              
                 
        conn = getConnectionRetry();  
        if (conn==null){
            isError = true;
            return null;
        }
        
        List<List<String>> values = new ArrayList<>();
        ResultSet rs = null;
         
        try {
         
           executeTime = System.currentTimeMillis();
           
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           rs = stmt.executeQuery(query);
                  
	   List<String> metaData = new ArrayList<>();

           ResultSetMetaData meta = rs.getMetaData();
     
           
           metaData.add("TYPE"); //add only
           metaData.add("NAME"); //add only            
           metaData.add("VALUE"); //add only 
           
           values.add(metaData);
         
           if (meta.getColumnCount()<4){
               isError = true;
               return null;
           }

           while (rs.next()) {
                   List<String> Data = new ArrayList<>();
                   String name = rs.getString(4);
                   String type = rs.getString(2);
                   String value= rs.getString(3); 
                   
                   if (name!=null && type!=null){
                        if (value==null) value="Unknowwn";
                        
                        name = name.replaceAll("\\s+", " ");
                        type = type.replaceAll("\\s+", " ");
                        value = value.replaceAll("\\s+", " ");
                                
                        name = name.replaceAll("\\s+", " ");
                        type = type.replaceAll("\\s+", " ");
                        
                        if (type.charAt(type.length()-1)==' '){
                            type = type.substring(0, type.length()-1);
                        }  
                        
                        if (name.charAt(name.length()-1)==' '){
                            name = name.substring(0, name.length()-1);
                        } 
                        
                        if (value.charAt(value.length()-1)==' '){
                            value = value.substring(0, value.length()-1);
                        } 
                        
                        if (type.equalsIgnoreCase("USER")) continue;
                      
                        if (name.length()<=1) continue;
                        
                        Data.add(type); 
                        Data.add(name);
                                            
                        Data.add(value);
                    }       
                                  
                   values.add(Data);
           }
           
             attemptClose(rs);
             attemptClose(stmt);   
        }   
     
        catch (SQLException e) {
               isError=true;
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
        } finally {
            executeTime = System.currentTimeMillis()-executeTime;
        }
       
       // if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single"))
        attemptClose(conn);
        
        return values;
    }
   
   public List<List<String>> getRepLiteThreadsValue(int _queue){
        
       String query ="admin who, no_trunc"; 
       this.__queue=_queue;
        
        if (_queue>sybaseApiList.size()){
            logger.error("ERROR detected in queue sizer!!!");
            isError=true; 
            errorDescr = "ERROR detected in queue sizer!!!";
            return null;
        }
        
        isError=false;  
                        
        conn = getConnectionRetry();  
        if (conn==null){
            isError = true;
            return null;
        }
        
        List<List<String>> values = new ArrayList<>();
        ResultSet rs = null;
         
        try {
            executeTime = System.currentTimeMillis();
                    
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           rs = stmt.executeQuery(query);
                  
	   List<String> metaData = new ArrayList<>();

           ResultSetMetaData meta = rs.getMetaData();
     
           
           metaData.add("TYPE"); //add only
           metaData.add("NAME"); //add only            
           metaData.add("VALUE"); //add only 
           
           values.add(metaData);
         
           if (meta.getColumnCount()<4){
               isError = true;
               return null;
           }

           while (rs.next()) {
                   List<String> Data = new ArrayList<>();
                   String name = rs.getString(4);
                   String type = rs.getString(2);
                   String value= rs.getString(3); 
                   
                   if (name!=null && type!=null){
                        if (value==null) value="Unknowwn";
                        
                        name = name.replaceAll("\\s+", " ");
                        type = type.replaceAll("\\s+", " ");
                        value = value.replaceAll("\\s+", " ");
                                
                        name = name.replaceAll("\\s+", " ");
                        type = type.replaceAll("\\s+", " ");
                        
                        if (type.charAt(type.length()-1)==' '){
                            type = type.substring(0, type.length()-1);
                        }  
                        
                        if (name.charAt(name.length()-1)==' '){
                            name = name.substring(0, name.length()-1);
                        } 
                        
                        if (value.charAt(value.length()-1)==' '){
                            value = value.substring(0, value.length()-1);
                        } 
                        
                        if (type.equalsIgnoreCase("USER") || type.equalsIgnoreCase("SQM") || type.equalsIgnoreCase("SQT")
                                || type.equalsIgnoreCase("dSUB") || type.equalsIgnoreCase("dCM") || type.equalsIgnoreCase("dAIO")
                                || type.equalsIgnoreCase("dREC") || type.equalsIgnoreCase("dALARM")|| type.equalsIgnoreCase("dSYSAM")) continue;
                      
                        if (name.length()<=1) continue;
                        
                        Data.add(type); 
                        Data.add(name);
                                            
                        Data.add(value);
                    }       
                                  
                   values.add(Data);
           }
           
             attemptClose(rs);
             attemptClose(stmt);   
        }   
     
        catch (SQLException e) {
               isError=true;
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
        } finally {
            executeTime = System.currentTimeMillis()-executeTime;
        }
             
        
        
        //if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single")) 
        attemptClose(conn);
        
        return values;
    }
    
    
    private List<String> getSingleValue(Connection conn, String query){
        
        List<String> val =  new ArrayList<>();
     
         if (conn == null){
            logger.error("Could not create connection to database server getPIDtoKil");
            return val;
         }
        
         ResultSet rs = null;
         
        try {
           executeTime = System.currentTimeMillis();
           Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
           rs = stmt.executeQuery(query);
                  
           while (rs.next())  {
              String pid = rs.getString(1);
              if (rs.wasNull()) {
              } else {
                  val.add(pid);
              }
               
           }
           attemptClose(rs);
           attemptClose(stmt);
        
        }   
     
        catch (SQLException e) {
               isError=true;
               logger.error("SQL Exception/Error:");
               logger.error("error message=" + e.getMessage());
               logger.error("SQL State= " + e.getSQLState());
               logger.error("Vendor Error Code= " + e.getErrorCode());
               logger.error("SQL Exception/Error: ", e);
        } finally {
            executeTime = System.currentTimeMillis()-executeTime;
        }
     
        return val;
    }
    
    private List<String> getPIDtoKill(Connection conn, String query){
        
        return getSingleValue(conn, query);
    }
    
    private Connection getConnectionRetry(String type, String host, int port, String user, String password, int timeout, int retry) {
        int licz_conn=0;
        Connection connection =null;
        while (licz_conn<=retry){
           connection = getConnection(type, host, port, user, password, timeout);
           if (connection==null){
               logger.error("Conn in SybaseCore is null RETRY " + licz_conn);
              //po nowemu retry
               try {
                   Thread.sleep(1000);
               } catch (InterruptedException ex) {
                   logger.error(ex);
               }
                licz_conn++;
            } else {
                licz_conn=retry+1;
                break;
            }
        }
        
        return connection;
  }
   //tylko dla compare
   private Connection getConnection(String type, String host, int port, String user, String password, int timeout) {
           Properties prop = new Properties();

           prop.setProperty("USER", user);
           prop.setProperty("PASSWORD", password);
           prop.setProperty("APPLICATIONNAME", "thread.single.sybase");
       
           if (type.equalsIgnoreCase("single")){
           
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
          } else {
               return null;
           }

     }

   private Connection getConnectionRetry() {
        int licz_conn=0;
        Connection connection =null;
        while (licz_conn<=sybaseApiList.get(__queue).getSybaseConnRetry()){
           connection = getConnection();
           if (connection==null){
                logger.error("Conn in SybaseCore is null RETRY " + licz_conn);
                licz_conn++;
            } else {
                licz_conn=sybaseApiList.get(__queue).getSybaseConnRetry()+1;
                break;
            }
        }
        
        return connection;
  }
   
   private Connection getConnection() {
           Properties prop = new Properties();

           prop.setProperty("USER", sybaseApiList.get(__queue).getSybaseUser());
           prop.setProperty("PASSWORD", sybaseApiList.get(__queue).getSybasePassword());
           prop.setProperty("APPLICATIONNAME", "thread.single.sybase");
           
           if (sybaseApiList.get(__queue).getInterfaceType().equalsIgnoreCase("single")){
               Connection connection = null;
               try {
                   //Add charset
                   Object sybDriver = Class.forName("com.sybase.jdbc4.jdbc.SybDriver").newInstance();
                   DriverManager.setLoginTimeout(sybaseApiList.get(__queue).getSybaseConnTimeout());
                   connection =  DriverManager.getConnection("jdbc:sybase:Tds:"+sybaseApiList.get(__queue).getSybaseHost()+":" + sybaseApiList.get(__queue).getSybasePort()+"?charset=utf8", prop);
               if(connection!=null){
                   logger.debug("Connected to " + sybaseApiList.get(__queue).getSybaseHost()+":"+sybaseApiList.get(__queue).getSybasePort());
               }
               return connection;
              } catch (ClassNotFoundException e) {
                     logger.error("ClassNotFoundException:");
                     logger.error("error message=" + e.getMessage());
                     return null;

              } catch (SQLException e) {
                  while(e != null) {
                                logger.error(sybaseApiList.get(__queue).getInterfaceType());
                                logger.error("getConnection SQL Exception/Error:");
                                logger.error("error message=" + e.getMessage());
                                logger.error("SQL State= " + e.getSQLState());
                                logger.error("Vendor Error Code= " + e.getErrorCode());

                                // it is possible to chain the errors and find the most
                                // detailed errors about the exception
                                e = e.getNextException( );
                   }
                   SQLWarning warning = null;
                   try {
                       // Get SQL warnings issued
                      warning = connection.getWarnings();
                      if (warning == null) {}
                      else {
                        while (warning != null) {
                          logger.warn("openSQLConnection - Warning: " + warning);
                          warning = warning.getNextWarning();
                        }
                      }
                    }
                    catch (Exception wex) {
                       logger.error(wex);
                    } // Unable to process SQL connection warnings
                  
                   return null;
              } catch (Exception e2) {
                // handle non-SQL exception …
                  logger.error("SQL Exception/Error:");
                  logger.error("error message=" + e2.getMessage());

                   return null;
              } 
          } else {
               logger.debug("POOLING Connected to " + sybaseApiList.get(__queue).getSybaseHost()+":"+sybaseApiList.get(__queue).getSybasePort());
               Connection connection = DataSource.getInstance(sybaseApiList.get(__queue).getSybasePoolId()).getConnection();
               try { 
                    if (sybaseApiList.get(__queue).getIsolationLevel()==-1) {

                       //   connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

                   } else {
                        connection.setTransactionIsolation(sybaseApiList.get(__queue).getIsolationLevel());
                   } 
               } catch (Exception ex) {
                       logger.error(ex);
               }
               return connection;
             
            
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
    
    public boolean isError(){
         return isError;
    }
 
    
}