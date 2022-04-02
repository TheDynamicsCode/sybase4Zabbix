/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.api;

/**
 *
 * @author szydlowskidom
 */
public class JdbcApi {
    
    private String interface_name;
    private String host;
    private int port;
    private String s_port;
    private String group;
    private String user;
    private String password;
    private boolean activeMode; 
    private boolean isCluster; 
    private String interface_type;
     
    private int conn_retry;
    private int pool_index;
    
    private int minPoolSize;
    private int maxPoolSize;
    private int maxStatements; 
    private int acquireIncrement;
    private int loginTimeout;
    
    public  JdbcApi(){
        interface_name="ifn";
        host="host";
        port=9000;
        s_port="9000";
        user="user";
        password="pswd";
        activeMode=true; 
        interface_type="single";
        group="default";
        
        conn_retry=1;
        pool_index=0;

        minPoolSize=5;
        maxPoolSize=10;
        maxStatements=180; 
        acquireIncrement=2;
        loginTimeout = 10;
        
        isCluster = false;
    }
    
    
    public boolean isCluster(){
        return isCluster;
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
    
    public void setInterfaceName(String set){
        interface_name=set;
    } 
    
    public void setInterfaceType(String set){
        interface_type=set;
    }
    
    public void setHost(String set){
        host=set;
        if (host.split(",").length>1) isCluster=true;
    } 
    
    
    public void setClusterOption(Boolean set){
        isCluster=set;
    }
    
    public void setPoolIndex(int _pool_index){
        pool_index=_pool_index;
    }
     
    
    public void setPort(String set){
        try {
            port=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
        s_port=set;
    }  
    
    public void setMinPoolSize(String set){
        try {
            minPoolSize=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
    
     public void setMaxPoolSize(String set){
        try {
            maxPoolSize=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
     
    public void setMaxStatements(String set){
        try {
            maxStatements=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
   
    public void setLoginTimeout(String set){
        try {
            loginTimeout=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
        if (loginTimeout<0) loginTimeout=1;
    }
     
     public void setAcquireIncrement(String set){
        try {
            acquireIncrement=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
    } 
    
 
    public void setConnectionRetry(String set){
        try {
            conn_retry=Integer.parseInt(set);
        } catch (NumberFormatException ignore){}
        if (conn_retry<0) conn_retry=0;
    } 
    
   
    public void setGroup(String set){
        group=set;
    }
    
    public void setUser(String set){
        user=set;
    }
     
    public void setPassword(String set){
        password=set;
    }
   
    public String getInterfaceName(){
        return interface_name;
    }
   
    public String getInterfaceType(){
        return interface_type;
    }
    
    public String getHost(){
        return host;
    }
    
    public int getPort(){
        return port;
    }
    
    public int getConnectionRetry(){
        return conn_retry;
    }
      
    public String getStringPort(){
        return s_port;
    }
    
    public String getGroup(){
        return group;
    }
    
    public String getUser(){
        return user;
    }
     
    public String getPassword(){
        return password;
    }
    
    public int getMinPoolSize(){
        return minPoolSize;
    }
    
    public int getMaxPoolSize(){
        return maxPoolSize;
    }
    
    public int getMaxStatements(){
        return maxStatements;
    }  
    
    public int getAcquireIncrement(){
        return acquireIncrement;
    }  
    
    public int getLoginTimeout(){
        return loginTimeout;
    }
    
    public int getPoolIndex(){
        return pool_index;
    }
 
    
}
