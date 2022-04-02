/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;

import biz.szydlowski.sybase4zabbix.api.JdbcApi;
import biz.szydlowski.sybase4zabbix.api.SybaseApi;
import biz.szydlowski.sybase4zabbix.api.SysmonApi;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Dominik
 */
public class WorkingObjects {
    
    public static List<SysmonApi> sysmonApiList  = null;
    public static List<SybaseApi> sybaseApiList = null;   
    public static List<JdbcApi> JdbcApiList = null;
    public static List<String> allowedConn = null;
    public static Map<String, String> CrontabGroupsList = null;
    
    public static SybaseQuartz _SybaseQuartz=null;
    public static SysmonQuartz _SysmonQuartz=null;  
    
    public static HashMap<String, Boolean> unique_hostname  = null;
    public static HashMap<String, Integer> JdbcGroup = null;
    
    public static void clear(){
        if (_SybaseQuartz!=null) _SybaseQuartz.stop();
        if (_SysmonQuartz!=null) _SysmonQuartz.stop();  
        
        if (sysmonApiList!=null) sysmonApiList.clear();
        if (sybaseApiList!=null) sybaseApiList.clear();
        if (JdbcApiList!=null) JdbcApiList.clear();
        if (allowedConn!=null) allowedConn.clear();
        if (CrontabGroupsList!=null)  CrontabGroupsList.clear();     

        if (unique_hostname!=null)  unique_hostname.clear();
        if (JdbcGroup!=null)  JdbcGroup.clear();
    }
    
    
    public static void destroy(){
        sysmonApiList  = null;
        sybaseApiList = null; 
        JdbcApiList = null;
        allowedConn = null;
        CrontabGroupsList = null;
        _SybaseQuartz=null;
        _SysmonQuartz=null;  

        unique_hostname  = null;
        JdbcGroup = null;
    
    }
    
}
