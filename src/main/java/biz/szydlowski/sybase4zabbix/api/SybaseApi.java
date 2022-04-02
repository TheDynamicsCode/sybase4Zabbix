/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.api;

import biz.szydlowski.sybase4zabbix.sqlType;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Dominik
 */
public class SybaseApi {
    
    private static int ZBX_STATE_NORMAL=0;
    private static int ZBX_STATE_NOTSUPPORTED=1;
    
    private boolean activeMode; 
    private String zabbix_server_name;
    private String alias;
    private String zabbix_key;
    private String zabbix_host;
    private sqlType sql_type;
    private int isolation_level;
    private String sql_query;
    private String discovery_query;
    private String complementary_query;
    private boolean isComplementary;
    private boolean fullquery; 
    private boolean is_html; 
    private boolean show_count; 
    private boolean is_integer; 
    private boolean is_float; 
    private boolean is_long; 
    private boolean is_string;
    private boolean timing;
    private boolean isReadyToSend; 
    private boolean isNowExecuting; 
    private boolean isNowRefreshing;
    private boolean isDiscoveryMetadataFromFile;
    private boolean isDiscoveryMapFromFile;
    private boolean isDiscoveryParamsTypeFromFile;
    private boolean isComplementaryMapFromFile;
    private long lastTimeComplementary;
    private long complementaryUpdateInterval;
    private boolean no_data;
    private int column;
    private int row;
    private int more_stmt;
    private String cron_expression;
    private String sybase_interface;
    private String sybase_host;
    private String sybase_port;
    private String sybase_user;
    private String sybase_password;
    private String sysmon_name; 
    private String interface_type;
    private int sybase_pool_id;
    private int sybase_conn_retry;
    private int sybase_conn_timeout;
    private final Properties prop;
    private long lastTimeDiscovery;
    private long discoveryUpdateInterval;
    private String lastExecuteTime;
    private String settingFile;
    private boolean fireAtStartup;
    private boolean sendTozbx;
    
    private long last_time_spending;
    private long time_spending;
    private long hits; 
    private long time_spending_dt;
    private long hits_dt;
    
    private final List<String> discoveryParamsType;
    private List<String> discoveryMetaData;
    private List<List<String>> discoveryMap;
    private List<List<String>> complementaryMap;
    private final List <String> returnValue;
    private final List <String> configErrorList;
    private final List <Integer> state;
    private final SimpleDateFormat sdf;
    
    public SybaseApi(){
        
        discoveryParamsType = new ArrayList<>();
        discoveryMetaData = new ArrayList<>();
        discoveryMap = new ArrayList<>();
        complementaryMap = new ArrayList<>();
        returnValue = new ArrayList<>();
        configErrorList = new ArrayList<>();
        state = new ArrayList<>();
        sdf = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss");
        
        isComplementary=false;
        lastExecuteTime = "NONE";
        this.activeMode=true;
        this.zabbix_server_name="name";
        this.alias="alias";
        this.zabbix_key="zkey";
        this.zabbix_host="zhost";
        this.sql_type=sqlType.PURE;
        this.sql_query="SELECT -5";
        this.sysmon_name="disabled";
        this.is_string=false; 
        this.is_integer=true; 
        this.is_float=false; 
        this.is_long=false;
        this.timing=false;
        this.isReadyToSend=false;
        this.isNowExecuting=false;
        this.isNowRefreshing=false;
        this.fullquery=false;
        this.cron_expression = "0 0 0 * * ?";
        this.isolation_level=-1;
        this.no_data=false;
        this.sendTozbx=true;
        
        this.column = 1;
        this.row=1;
        this.more_stmt=1; 
        this.is_html=false; 
        this.show_count=true;
        
        this.sybase_host="s_host";
        this.sybase_port="5010";
        this.sybase_user="usr";
        this.sybase_password="pswd";
        this.sybase_interface="inf";  
        this.sybase_conn_retry=1;
        this.sybase_conn_timeout=5;
        
        this.discovery_query = "select 1";
        this.complementary_query = "select 1";
        this.isDiscoveryMetadataFromFile=false;
        this.isDiscoveryMapFromFile = false;
        this.isDiscoveryParamsTypeFromFile=false;
        this.isComplementaryMapFromFile=false;
        this.fireAtStartup=false;
        this.time_spending=0L;
        this.hits=0L; 
        
        this.time_spending_dt=0L;
        this.last_time_spending=0L;
        this.hits_dt=0L;
        
       interface_type="single";
       sybase_pool_id=-1;
       sybase_conn_retry=1;
       sybase_conn_timeout=5;
       prop = new Properties();
       lastTimeDiscovery = 0L;
       discoveryUpdateInterval=30000L;
       
        lastTimeComplementary = 0L;
        complementaryUpdateInterval=30000L;
        
        state.clear();
        returnValue.clear();
        discoveryMetaData.clear();
        discoveryParamsType.clear();
        configErrorList.clear();
        settingFile = "default";
    }
    
    public void resetDtCounter(){
        this.time_spending_dt=0L;
        this.hits_dt=0L;
    }   
    
    public long getTimeSpending(){
        return time_spending;
         
    }   
    
    public long getLastTimeSpending(){
        return last_time_spending;
         
    }
    
    public long getNumberOfHits(){
        return hits;
         
    }   
    
    public long getTimeSpendingDt(){
        return time_spending_dt;
         
    }   
    
    public long getNumberOfHitsDt(){
        return hits_dt;
         
    }    
   
    public synchronized void setTimeSpending(long liczba){
          last_time_spending = liczba;
          time_spending = time_spending+liczba;
          //obliczenia do HTS RATIO
          time_spending_dt = time_spending_dt+liczba;          
          if (liczba>0) {
              hits++;
              hits_dt++;
          }
    }

    public void setSettingFile(String settingFile){
        this.settingFile=settingFile;
    }
    
    public void setToActiveMode(){
        activeMode=true;
    }
    
    public void setToInactiveMode(){
        activeMode=false;
    } 
    
    public boolean isActiveMode(){
       return activeMode;
    } 
    
    public void setZabbixServerName(String set){
        zabbix_server_name=set;
    } 
    
    public void setAlias(String set){
        alias=set;
    } 
    
    public void setZabbixKey(String set){
        zabbix_key=set;
    } 
    
    public void setZabbixHost(String set){
        zabbix_host=set;
    } 
           
    public boolean isFireAtStartup(){
       return  fireAtStartup;
    }    
    
    public boolean isSendToZbx(){
       return  sendTozbx;
    } 
      
    
    public void setFireAtStartup(String set){
         fireAtStartup=Boolean.parseBoolean(set);    
    }
    
    public void setSentToZbx(String set){
         sendTozbx=Boolean.parseBoolean(set);    
    }
    
    public void setSqlType(sqlType set){
         sql_type=set;
    } 
    
    public void setSqlTypeFromString(String set){
       sql_type = sqlType.valueOf(set);
    }
    
    public void setIsolationLevel(String set){
        try {
            isolation_level=Integer.parseInt(set);
        } catch (NumberFormatException ignore){
        }
    }
    
    public void setSqlQuery(String set){
        sql_query=set;
    }  
    
    public void setDiscoveryQuery(String set){
        discovery_query=set;
    } 
    public void setIsString(String set){
         is_string=Boolean.parseBoolean(set);
         if (is_string){
              is_integer=false;
              is_long=false;
              is_float=false;
         }      
    }  
     //deprecated
     public void setIsNumeric(String set){
         is_integer=Boolean.parseBoolean(set);
         if (is_integer) is_string=false;
    }  
    
    public void setIsInteger(String set){
         is_integer=Boolean.parseBoolean(set);
         if (is_integer){
             is_string=false;
             is_long=false;
             is_float=false;
         }
         
    }  
    
     public void setIsLong(String set){
         is_long=Boolean.parseBoolean(set);
         if (is_long){
             is_string=false;
             is_integer=false;
             is_float=false;
         }
        
    }   
    
    
    public void setIsFloat(String set){
         is_float=Boolean.parseBoolean(set);  
         if (is_float){
             is_string=false;
             is_integer=false;
             is_long=false;
         }
    }   
    
    public void setHtml(String set){
        is_html=Boolean.parseBoolean(set);
    } 
    
    public void setShowCount(String set){
        show_count=Boolean.parseBoolean(set);
    } 
    
    public void setIsTiming(String set){
        timing=Boolean.parseBoolean(set);
    } 
    
    public void setNoData(String set){
        no_data=Boolean.parseBoolean(set);
    } 
    
    public void setFullQuery(String set){
        fullquery=Boolean.parseBoolean(set);
         if (fullquery){
             is_string=false;
             is_integer=false;
             is_long=false;
             is_float=false;
        }
    }  
        
    public void setColumn(String set){
        try {
            column=Integer.parseInt(set);
        } catch (NumberFormatException ignore){
           column=1;
        }
    }
   
    public void setRow(String set){
        try {
            row=Integer.parseInt(set);
        } catch (NumberFormatException ignore){
            row=1;
        }
    }
   
    public void setMoreStmt(String set){
        try {
            more_stmt=Integer.parseInt(set);
        } catch (NumberFormatException ignore){
            more_stmt=1;
        }
    }  
    
    public void setCronExpression(String set){
        if (org.quartz.CronExpression.isValidExpression(set)){
           cron_expression=set;
        } else {
            cron_expression = "0 0 0 * * ?"; 
        }
    } 
        
    public void setSybaseHost(String set){
       sybase_host=set;
    }
    
    public void setSybasePort(String set){
       sybase_port=set;
    }
    
    public void setSybaseUser(String set){
       sybase_user=set;
    } 
    
    public void setSybasePassword(String set){
       sybase_password=set;
    } 
    
    public void setSybaseInterface(String set){
       sybase_interface=set;
    } 
    
    public void setInterfaceType(String set){
       interface_type=set;
    } 

    public String getInterfaceType(){
       return interface_type;
    } 
    
    public void setSybasePoolId(int set){
       sybase_pool_id=set;
    } 
    
    public int getSybasePoolId(){
       return sybase_pool_id;
    } 
    
    public void setSybaseConnRetry(int set){
       sybase_conn_retry=set;
    } 
    
    public void setSybaseConnTimeout(int set){
       sybase_conn_timeout=set;
    }
    
    public void addErrorToConfigErrorList(String error){
       configErrorList.add(error);
    }  
    
    public void addErrorToConfigErrorList(List<String> error){
        error.forEach(configErrorList::add);
    }
   
    public String printConfigErrorList(){
        StringBuilder sb = new StringBuilder();
         sb.append("<font color=\"red\">");
        configErrorList.forEach((e) -> {
            sb.append(e).append("<br>");
        }); 
        sb.append("</font>");
        return sb.toString();
    }
    
 
    public void setJdbcDriver(JdbcApi jdbc_driver){
          setSybaseHost(jdbc_driver.getHost());
          setSybasePort(jdbc_driver.getStringPort());
          setSybaseUser(jdbc_driver.getUser());
          setSybasePassword(jdbc_driver.getPassword());
          setSybaseInterface(jdbc_driver.getInterfaceName());
          setInterfaceType(jdbc_driver.getInterfaceType());
          setSybasePoolId(jdbc_driver.getPoolIndex());
          setSybaseConnRetry(jdbc_driver.getConnectionRetry());
          setSybaseConnTimeout(jdbc_driver.getLoginTimeout());           
    }
    
    public void setDatabaseProperty(String key, String value){
        prop.put(key, value);
    }
    
    public void setSysmonName(String set){
       sysmon_name=set;
    }
    
    public void setReadyToSendToTrue(){
        isReadyToSend=true;
    } 
    
    public void setReadyToSendToFalse(){
        isReadyToSend=false;
    }
   
    public void setIsNowExecuting (){
        isNowExecuting = true;
        lastExecuteTime = sdf.format(new Date());
    }  
    
    public String getLastExecuteTime(){
        return lastExecuteTime;
    }
    
    public void setIsNowRefreshing (){
        isNowRefreshing= true;
    } 
 
 
    public void setIsNowExecutingToFalse (){
        isNowExecuting = false;
    } 
    
    public void setIsNowRefreshingToFalse (){
        isNowRefreshing = false;
    } 
    
 
    public void clearReturnAndStateValue(){
        returnValue.clear();
        state.clear();
    }
     
    public void addNormalReturnValue(String value){
        addReturnValue(value);
        state.add(ZBX_STATE_NORMAL);
    }
    public void addErrorReturnValue(String value){
        addReturnValue(value);
        state.add(ZBX_STATE_NOTSUPPORTED);
    }
    private void addReturnValue(String value){
        returnValue.add(value);
    }
    
   public void setDiscoveryMapAndMetaData(List<List<String>> _discoveryMap){
        discoveryMap.clear();
        setTimeDiscovery();
        if (_discoveryMap.size()>0){
            if (isDiscoveryMetadataFromFile) {
                 _discoveryMap.remove(0);
            } else {
                //dodaj meata data
                discoveryMetaData.clear();
                discoveryMetaData =  _discoveryMap.remove(0);
            }
        }
        this.discoveryMap = _discoveryMap;
    } 

   
    
    public void setDiscoveryMapAndMetaData(String spl){
      isDiscoveryMapFromFile = true; 
      discoveryMap.clear();
      setTimeDiscovery();
      String [] lines = spl.split(";;");
      setDiscoveryMetaData(lines[0]);
      for (int i=1; i<lines.length;i++){
            List<String> discoveryline = new ArrayList<>();
            String [] tmp = lines[i].split(";");
            discoveryline.addAll(Arrays.asList(tmp));
            discoveryMap.add(discoveryline);
       }
    } 
    
    
    public void setDiscoveryMap(String spl){
      isDiscoveryMapFromFile = true;
      discoveryMap.clear();
       setTimeDiscovery();
       String [] lines = spl.split(";;");
       for (String line : lines){
            List<String> discoveryline = new ArrayList<>();
            String [] tmp = line.split(";");
            discoveryline.addAll(Arrays.asList(tmp));
            discoveryMap.add(discoveryline);
        }
    } 
    
    public void setDiscoveryMetaData(String spl){
        isDiscoveryMetadataFromFile = true;
        discoveryMetaData.clear();
        String [] adder = spl.split(";");
        discoveryMetaData.addAll(Arrays.asList(adder));
    }  
    
    public void setDiscoveryMetaData(List<String> meta){
        isDiscoveryMetadataFromFile=false;
        discoveryMetaData.clear();
        discoveryMetaData = meta;
    } 
    

    public void setDiscoveryParamsType(String spl){
        isDiscoveryParamsTypeFromFile=true;
        discoveryParamsType.clear();
        String [] adder = spl.split(";");
        discoveryParamsType.addAll(Arrays.asList(adder));
    } 
    
 
   
    
    public void setComplementaryMapAndMetaData(String spl){
      setTimeComplementary(); 
      isComplementaryMapFromFile = true;
      isComplementary=true;
      complementaryMap.clear();
      String [] lines = spl.split(";;");
      for (int i=0; i<lines.length;i++){
            List<String> compline = new ArrayList<>();
            String [] tmp = lines[i].split(";");
            compline.addAll(Arrays.asList(tmp));
            complementaryMap.add(compline);
       }
    } 
    
    public String getSettingFile(){
        return this.settingFile;
    }
    
    public Properties getDatabaseProperties(){
        return prop;
    }  
    
    public String getDatabaseProperty(String key, String default_value){
        return prop.getProperty(key, default_value);
    }
   
    public String getAlias(){
        return alias;
    } 
    
    public sqlType getSqlType(){
         return sql_type;
    }  
    
    public String getSqlQuery(){
        return sql_query;
    }  
    
     public String getDiscoveryQuery(){
        return discovery_query;
    }  
     
     public List<List<String>> getDiscoveryMap(){
        return discoveryMap;
    }   
     
    public List<String> getDiscoveryMetaData(){
        return discoveryMetaData;
    }     
    
    public List<String> getDiscoveryParamsType(){
        return discoveryParamsType;
    } 
     
    public String getSybaseInterface(){
       return sybase_interface;
    }
    
    public String getSybaseHost(){
       if (sybase_host.equals("s_host")) {
           System.err.println("ERROR s_host in interface " + sybase_interface);
       }
       return sybase_host;
    }
    
    public String getSybaseStringPort(){
       return sybase_port;
    } 
    
    public int getSybasePort(){
        try {
           return Integer.parseInt(sybase_port);
        }   catch (NumberFormatException e) {
            return 9999;
        }
    }   
     
    public String getSybaseUser(){
       return sybase_user;
    } 
    
    public String getSybasePassword(){
       return sybase_password;
    }  
    
    public int getSybaseConnRetry(){
       return sybase_conn_retry;
    } 
    
    public int getSybaseConnTimeout(){
       return sybase_conn_timeout;
    }
   
    public int getIsolationLevel(){
        return isolation_level;     
    }
       
   
    public String getZabbixKey(){
        return zabbix_key;
    }   
    
    public String getZabbixHost(){
        return zabbix_host;
    } 
   
    public String getZabbixServerName(){
        return zabbix_server_name;
    }   

    
    public String getCronExpression(){
        return cron_expression;
    }   
    
    public String getSysmonName(){
       return sysmon_name;
    }
  
    
    public boolean isReadyToSend(){
        return isReadyToSend;
    } 
    
    public boolean isNowExecuting(){
        return isNowExecuting;
    }
   
    public boolean isNowRefreshing(){
        return isNowRefreshing;
    }
    
    public boolean isFullQuery(){
        return fullquery;
    }
    
   
    public boolean isInteger(){
        return is_integer;
    }  
    
    public boolean isLong(){
        return is_long;
    }  
    public boolean isFloat(){
        return is_float;
    }  
    
   
    public boolean isHtml(){
        return is_html;
    }  
    public boolean isShowCount(){
        return show_count;
    }  
    
    public boolean isTiming(){
        return timing;
    } 
    
    public boolean isNoData(){
        return no_data;
    }        
         
    public int getColumn(){
        return column;
    }
   
    public int getRow(){
        return row;
      
    }
   
    public int getMoreStmt(){
       return more_stmt;
    }  
    
  
    public List <String>  getReturnListValue(){
        if (returnValue.isEmpty()){
            state.clear();
            addErrorReturnValue("ERROR in getReturnListValue");
        }
        return returnValue;
    }     
    
    public List <String>  getDiagnosticReturnListValue(){
        return returnValue;
    }   
    
    
    public List <Integer>  getStateList(){
        if (state.isEmpty()){
            returnValue.clear();
            addErrorReturnValue("ERROR in getReturnListState");
        }
        return state;
    }
    
    public long getDiscoveryUpdateInterval(){
        return discoveryUpdateInterval;
    }  
    
    public void setDiscoveryUpdateInterval(long v){
        discoveryUpdateInterval=v;
    } 
    
    private void setTimeDiscovery(){
        lastTimeDiscovery = System.currentTimeMillis() ;
    } 
  
    private void setTimeComplementary(){
        this.lastTimeComplementary = System.currentTimeMillis() ;
    }
            
    public boolean isDiscoveryParamsTypeFromFile (){
       return isDiscoveryParamsTypeFromFile;
    }
  
    public boolean isDiscoveryMapFromFile(){
        return isDiscoveryMapFromFile;
    }  
    
    public boolean isDiscoveryMetadataFromFile(){
        return isDiscoveryMetadataFromFile;
    }
     
    public boolean mustDiscoveryNow (){
        if (isDiscoveryMapFromFile) {
            return false;
        } else {
             long timer = (System.currentTimeMillis() - lastTimeDiscovery)/1000;
            return timer > discoveryUpdateInterval;
        }
    }
    
    public boolean mustComplementaryNow (){
        if (isComplementaryMapFromFile) {
            return false;
        } else {
             long timer = (System.currentTimeMillis() - lastTimeComplementary)/1000;
            return timer > complementaryUpdateInterval;
        }
    }

    /**
     * @return the complementMap
     */
    public List<List<String>> getComplementaryMap() {
        return complementaryMap;
    }

    /**
     * @param complementMap the complementMap to set
     */
    public void setComplementaryMap(List<List<String>> complementMap) {
        setTimeComplementary();
        isComplementaryMapFromFile=false;
        this.complementaryMap = complementMap;
    }

    /**
     * @return the complement_query
     */
    public String getComplementary_query() {
        return complementary_query;
    }

    /**
     * @param complement_query the complement_query to set
     */
    public void setComplementary_query(String complement_query) {
        this.complementary_query = complement_query;
         setIsComplementary(true);
    }

    /**
     * @return the isComplement
     */
    public boolean isComplementary() {
        return isComplementary;
    }
    
    

    /**
     * @param isComplement the isComplement to set
     */
    public void setIsComplementary(boolean isComplement) {
        this.isComplementary = isComplement;
    }

    /**
     * @return the isComplementaryMapFromFile
     */
    public boolean isComplementaryMapFromFile() {
        return isComplementaryMapFromFile;
    }

    /**
     * @return the lastTimeComplementary
     */
    public long getLastTimeComplementary() {
        return lastTimeComplementary;
    }


    /**
     * @return the complementaryUpdateInterval
     */
    public long getComplementaryUpdateInterval() {
        return complementaryUpdateInterval;
    }

    /**
     * @param complementaryUpdateInterval the complementaryUpdateInterval to set
     */
    public void setComplementaryUpdateInterval(long complementaryUpdateInterval) {
        this.complementaryUpdateInterval = complementaryUpdateInterval;
    }
    
    
    
}
