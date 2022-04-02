/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.api;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author szydlowskidom
 */
public class SysmonApi {
   
    private String name;
    private String cron_expression;
    private String sybase_interface;
    private String sybase_host;
    private String sybase_port;
    private String sybase_user;
    private String sybase_password;
    private int sybase_conn_retry;
    private int sybase_conn_timeout;
    private String interface_type;
    private int sybase_pool_id;
    
    private int sample_interval_ms;
    private boolean isSetJdbc;
    private int error_setting_code;
    private boolean activeMode;  
    private boolean useMonTable; 
    
    private boolean isNowExecuting; 
    private boolean isNowRefreshing;
    private final List <String> configErrorList;
    
    private long time_spending;
    private long hits; 
    private long time_spending_dt;
    private long hits_dt;
    
    public SysmonApi() {
        
        name="name";
        cron_expression="0 0 0 * * ?";
        sybase_conn_retry=1;
        sybase_conn_timeout=5;
        interface_type="single";
        sybase_pool_id=-1;
        
        
        this.activeMode=true;
        this.sybase_interface = "inf";
        this.sybase_host = "host";
        this.sybase_user = "user";
        this.sybase_port = "5000";
        this.sybase_password = "pswd";
        this.sybase_conn_retry=1;
        this.sybase_conn_timeout=5;
        
        this.isNowExecuting=false;
        this.isNowRefreshing=false;
        configErrorList = new ArrayList<>();
        
        sample_interval_ms=1000;
        isSetJdbc = false;
        error_setting_code=0;  
        
        this.time_spending=0L;
        this.hits=0L; 
        
        this.time_spending_dt=0L;
        this.hits_dt=0L;
        this.useMonTable = false;
    } 
    
    public void resetDtCounter(){
        this.time_spending_dt=0L;
        this.hits_dt=0L;
    }   
    
    public long getTimeSpending(){
        return time_spending;
         
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
   
    public synchronized void setTimeSpendingAndHits(long liczba1, long liczba2){
         time_spending = time_spending+liczba1;
         time_spending_dt = time_spending_dt+liczba1;
         if (liczba1>0){
             hits=hits+liczba2;
             hits_dt=hits_dt+liczba2;  
         }          
          
    }
    
    public void setUseMonTable(String set){
           useMonTable=Boolean.parseBoolean(set);
    }
    
    public boolean isUseMonTable(){
           return useMonTable;
    }
  

    public void setToActiveMode(){
        activeMode=true;
    }
    
    public void setToInactiveMode(){
        activeMode=false;
    }    
   
    public void setIsNowExecuting (){
        isNowExecuting = true;
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
    
    public boolean isActiveMode(){
       return activeMode;
    } 
    
      
    public void setName(String set){
       name=set;
    }

    public void setCronExpression(String set){
        if (org.quartz.CronExpression.isValidExpression(set)){
           cron_expression=set;
        } else {
           cron_expression = "0 0 0 * * ?"; 
           error_setting_code = error_setting_code + 2;
        }
    }
    
    public void setSybaseInterface(String set){
       isSetJdbc=true;
       sybase_interface=set;
    }
  
    public void setSybaseHost(String set){
       isSetJdbc=true;
       sybase_host=set;
    }
    
    public void setSybasePort(String set){
       sybase_port=set;
    } 
    
    public void setSybaseConnRetry(int set){
       sybase_conn_retry=set;
    } 
    
    public void setSybaseConnTimeout(int set){
       sybase_conn_timeout=set;
    }
    
    
    public void setSampleIntervalMs(String set){
        try {
            sample_interval_ms=Integer.parseInt(set);
        } catch (NumberFormatException e){}
    }
    
    
    public void setSybaseUser(String set){
       sybase_user=set;
    } 
    
    public void setSybasePassword(String set){
       sybase_password=set;
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
        
    public void addErrorToConfigErrorList(String error){
       configErrorList.add(error);
    }  
       
    public List<String> getErrorList(){
       return configErrorList;
    }  
  
    public String getName(){
       return name;
    }
   
    public String getCronExpression(){
       return cron_expression;
    }
    
    public String getSybaseInterface(){
       return sybase_interface;
    }
    
    public String getSybaseHost(){
       return sybase_host;
    }
    
    public String getSybaseStringPort(){
       return sybase_port;
    } 
    
    public int getSybasePort(){
        int ret=5000;
        try {
           ret = Integer.parseInt(sybase_port);
        } catch (NumberFormatException ignore){}
        return ret;
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
    
    public boolean isSetJdbc(){
        return isSetJdbc;
    }   
    
    public boolean isNowExecuting(){
        return isNowExecuting;
    }
   
    public boolean isNowRefreshing(){
        return isNowRefreshing;
    }
    
    public int getSampleIntervalMs(){
        return  sample_interval_ms;
    }
    
    public int getErrorSettingCode(){
        if (!isSetJdbc)  error_setting_code = error_setting_code + 1;
        return error_setting_code;
    } 
 
    
}
