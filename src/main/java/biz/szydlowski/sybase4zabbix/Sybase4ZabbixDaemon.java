/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;

/**
 *
 * @author szydlowskidom
 */

import static biz.szydlowski.sybase4zabbix.WorkingObjects.*;
import biz.szydlowski.sybase4zabbix.api.DataSource;
import biz.szydlowski.sybase4zabbix.api.JdbcApi;
import biz.szydlowski.sybase4zabbix.api.SybaseApi;
import biz.szydlowski.sybase4zabbix.config.CrontabGroups;
import biz.szydlowski.sybase4zabbix.config.SybaseJdbc;
import biz.szydlowski.sybase4zabbix.config.SybaseParams;
import biz.szydlowski.sybase4zabbix.config.SysmonServer;
import biz.szydlowski.sybase4zabbix.config.WebParams;
import biz.szydlowski.sybase4zabbix.timers.ActiveAgentTask;
import biz.szydlowski.sybase4zabbix.web.WebServer;
import biz.szydlowski.utils.Constans;
import biz.szydlowski.utils.Memory;
import biz.szydlowski.utils.OSValidator;
import biz.szydlowski.utils.WorkingStats;
import biz.szydlowski.utils.tasks.MaintananceTask;
import biz.szydlowski.utils.tasks.TasksWorkspace;
import static biz.szydlowski.utils.tasks.TasksWorkspace.absolutePath;
import biz.szydlowski.utils.tasks.UpdateServer;
import biz.szydlowski.utils.tasks.WatchDogTask;
import biz.szydlowski.zabbixmon.ZabbixServer;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import com.mchange.v2.log.MLevel;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.sql.SQLException;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;


//http://www.neilson.co.za/creating-a-java-daemon-system-service-for-debian-using-apache-commons-jsvc/

/**
 * A simple Swing-based client for the capitalization server.
 * It has a main frame window with a text field for entering
 * strings and a textarea to see the results of capitalizing
 * them.
 */
public class Sybase4ZabbixDaemon implements Daemon {
    
    static {
        try {
            System.setProperty("log4j.configurationFile", getJarContainingFolder(Sybase4ZabbixDaemon.class)+"/setting/log4j/log4j2.xml");
        } catch (Exception ex) {
        }
    }
    
   
   
    
    static final Logger logger =  LogManager.getLogger(Sybase4ZabbixDaemon.class);
    private static boolean stop = false;
    private WebServer _WebServer = null;

    public static List <ZabbixServerApi> zabbixServerApiList = null;
    public static List<SysmonEngine> SysmonEngineList = null;
        
    public static Timer ActiveAgentTimer = new Timer("ActiveAgentProcessing", true);
    //private Timer ClusterMaintananceTimer = new Timer("ClusterMaintananceTimer", true);
    private Timer WatchdogTimer = new Timer("Watchdog", true);
    public static Timer  MaintenanceTimer = new Timer("MaintenanceTask", true);
    
        
    public static int ACTIVE_AGENT_TIME = 10000;
    public static Scheduler scheduler; 
    public static Scheduler scheduler_sysmon; 
    public static String APP_NAME="DEFAULT"; 
    
    public Sybase4ZabbixDaemon(){
    
    }
    
    public Sybase4ZabbixDaemon(boolean test, boolean win){
           System.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "WARNING");
           System.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
           System.setProperty("java.net.preferIPv4Stack", "true");
         
          if (test || win){
            if (!win) System.out.println("****** TESTING MODE  ********"); 
            else System.out.println("****** WINDOWS MODE  ********"); 
            try {
                jInit();
                start();
            } catch (Exception ex) {
                logger.error(ex);
                ex.printStackTrace();
            }
        }
    }
    
    
     public void jInit() {          
         
          
          JdbcGroup = new HashMap<>();
          
         
          if (OSValidator.isUnix()){
              absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"))+"/";
           } else {
               absolutePath="";
           }
          
          new UpdateServer("http://www.update.szydlowski.biz/software/sybase4Zabbix",Version.getBuild(),Version.getAgentVersion()).start();
            
           
          try {
             scheduler = new StdSchedulerFactory().getScheduler();
             scheduler_sysmon = new StdSchedulerFactory().getScheduler();
          } catch(SchedulerException e){
              logger.error("SchedulerException" + e);
          }
                   
           TasksWorkspace.start(Version.DEV_VERSION_EXPIRE_DATE, true);           
           WorkingStats.start();  
                                    
           printStarter();
                   
           new CrontabGroups().initData();
           
           JdbcApiList = new SybaseJdbc().getJdbcApiList();
           
           int fnd=0;
           for (int i=0; i<JdbcApiList.size();i++){
               fnd=0;
               if (JdbcGroup.getOrDefault(JdbcApiList.get(i).getGroup(), -1)==-1){
                   for (int j=0; j<JdbcApiList.size();j++){
                       if (JdbcApiList.get(j).getGroup().equals(JdbcApiList.get(i).getGroup())){
                           fnd++;  
                       }
                   }
                   if (fnd>0) JdbcGroup.put(JdbcApiList.get(i).getGroup(), fnd);
               }
           }
           
           sysmonApiList = new SysmonServer().getSysmonApiList();
        
           _SybaseQuartz = new SybaseQuartz();
           _SysmonQuartz = new SysmonQuartz();
           
           zabbixServerApiList = new ZabbixServer(absolutePath +"setting/zabbix-server.xml").getZabbixServerApiList();                        
           sybaseApiList = new SybaseParams().getSybaseApiList();
          
           WatchdogTimer.schedule(new WatchDogTask(), 10000, 2000);
           ActiveAgentTimer.schedule(new ActiveAgentTask(), 5000, ACTIVE_AGENT_TIME);
         
           MaintenanceTimer.schedule(new MaintananceTask(), 60000, 60000);
            
           unique_hostname = new HashMap<>();
               zabbixServerApiList.forEach((zabbixServerApi) -> {
                   setUniqZabbixHostnameForAgent(zabbixServerApi.getServerName());
            });
             
            initDataSource();
        
            WebParams _WebParams = new WebParams();
            if (_WebParams.isWebConsoleEnable()){ 
                 allowedConn = _WebParams.getAllowedConn();
                _WebServer = new WebServer(_WebParams.getWebConsolePort());
                _WebServer.setMaxConnectionCount(_WebParams.getWebMaxConnectionCount());
                _WebServer.start();
            }
                 
                        
            Memory.start();
                   
             
    }
     
    private void initDataSource(){
  
        
        for (JdbcApi _JdbcApi : JdbcApiList){   
            if (_JdbcApi.getInterfaceType().equals("pool")){
                logger.info("Adding pool id=" + _JdbcApi.getPoolIndex() + " " + _JdbcApi.getInterfaceName() + " - " + _JdbcApi.getHost() + " : " +  _JdbcApi.getStringPort());
                try {
                
                    DataSource.addInstance(_JdbcApi);
              
                } catch (SQLException e) {
                    logger.error(e);
                    //e.printStackTrace();
                } catch (IOException ex) {
                    logger.error(ex);
                    //ex.printStackTrace();
                } catch (PropertyVetoException ex) {
                    logger.error(ex);
                    //ex.printStackTrace();
                } finally {
                }
            }
        }
            
    } 
    
    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
            System.out.println("**** Daemon init *****");
                     
            handleCmdLine(daemonContext.getArguments());      
            
            String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
               absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
            
           
            jInit(); 
            
            logger.debug("Current path " + absolutePath);
            
   }

    @Override
    public void start() throws Exception { 
         logger.info("Starting daemon");
         _SybaseQuartz.doJob();
         _SysmonQuartz.doJob();
         logger.info("Started daemon");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stoppping daemon");
        _WebServer.stopSever();
        
        WorkingObjects.clear(); 
                   
        ActiveAgentTimer.cancel();
        MaintenanceTimer.cancel();
        //ClusterMaintananceTimer.cancel();
             
        Memory.stop();
        
        logger.info("Stopped daemon");
    }
   
    @Override
    public void destroy() { 
        logger.info("Destroying daemon");
       
         WorkingObjects.destroy();
        
         ActiveAgentTimer = null;
         MaintenanceTimer = null;
        // ClusterMaintananceTimer = null;
    }
    //https://support.google.com/gsa/answer/6316721?hl=en
    public static void start(String[] args) {
        System.out.println("start");
        Sybase4ZabbixDaemon sybase4ZabbixDaemon = new Sybase4ZabbixDaemon(false, true);
        
        while (!stop) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }
 
    public static void stop(String[] args) {
        System.out.println("stop");
        stop = true;
       
        logger.info("Stoppping daemon");
           
              
        Memory.stop();
        
        WorkingObjects.clear();
      
        
        logger.info("Stopped daemon");  
        
        System.exit(0);
                
    }
 
   
    
    public static void main(String[] args)  {
        
         if (args.length>0){
             if (args[0].equalsIgnoreCase("testing")){
                 Sybase4ZabbixDaemon sybase4ZabbixDaemon = new Sybase4ZabbixDaemon(true, false);
             }

         }

    }
    
   
    private static void printStarter(){
        logger.info(Constans.STARTER);
        logger.info(new Version().getAllInfo());
    }
    
  
 
    
      
     public void handleCmdLine(String[] args) {
      
         for (int i = 0; i < args.length; i++) {
           String arg = args[i];
           if (arg.regionMatches(0, "-", 0, 1))  {
            try {
               switch (arg.charAt(1)) { 
               case 'n':
                 i++;
                 break;
                 
               case 'a' :
                  i++;
                  APP_NAME = args[i];
                  break;             

               default:
                 //printUsage(language);
               }
             }
             catch (ArrayIndexOutOfBoundsException ae)  {
              // printUsage(language);
             }
           }
        }
     }
     
    
    private void setUniqZabbixHostnameForAgent(String zabbix_host){
                     
        String ag="";
        String name="";
        
        for (SybaseApi prop : sybaseApiList){
             ag = zabbix_host+"."+prop.getZabbixHost();
             name = prop.getZabbixServerName();
          
            unique_hostname.put(ag, true);
         
        }
        
    }
    
        
    public static String getJarContainingFolder(Class aclass) throws Exception {
          CodeSource codeSource = aclass.getProtectionDomain().getCodeSource();

          File jarFile;

          if (codeSource.getLocation() != null) {
            jarFile = new File(codeSource.getLocation().toURI());
          }
          else {
            String path = aclass.getResource(aclass.getSimpleName() + ".class").getPath();
            String jarFilePath = path.substring(path.indexOf(":") + 1, path.indexOf("!"));
            jarFilePath = URLDecoder.decode(jarFilePath, "UTF-8");
            jarFile = new File(jarFilePath);
          }
          return jarFile.getParentFile().getAbsolutePath();
     }
    
          

}