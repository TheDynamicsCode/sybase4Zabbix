/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;


import static biz.szydlowski.sybase4zabbix.Sybase4ZabbixDaemon.SysmonEngineList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.sybaseApiList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.sysmonApiList;
import biz.szydlowski.utils.WorkingStats;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.UnableToInterruptJobException;

/**
 *
 * @author dominik
 */
public class SysmonQuartzJob implements InterruptableJob {
    
   
    static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.SysmonQuartzJob");
   
    private volatile Thread  thisThread;    
    private JobKey   jobKey   = null; 
    private volatile boolean isJobInterrupted = false;
    
    private SysmonEngine _sp_sysmon = null;  

  
    public  SysmonQuartzJob(){      
         
    }

    
    @Override
    public void execute(JobExecutionContext jeContext) throws JobExecutionException {
            DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.GERMAN);
   
            otherSymbols.setDecimalSeparator('.');
            otherSymbols.setGroupingSeparator('.'); 
                 
            DecimalFormat df = new DecimalFormat("###.###", otherSymbols);
    
    
            thisThread = Thread.currentThread();
           // logger.info("Thread name of the current job: " + thisThread.getName());
             
                     
            jobKey = jeContext.getJobDetail().getKey();
            logger.debug("Job " + jobKey + " executing at " + new Date());
            
         
            int sysmon_api_queue = 0; 
            boolean stillExecuting=false;
                   
            try {

                   JobDataMap jdMap = jeContext.getJobDetail().getJobDataMap();
                
                           
                    try {
                        sysmon_api_queue = Integer.parseInt(jdMap.get("sysmon_api_queue").toString());
                    } catch (NumberFormatException e){}
                    
                    if (!sysmonApiList.get(sysmon_api_queue).isActiveMode()){
                        WorkingStats.idlePlus();
                    } else   if (sysmonApiList.get(sysmon_api_queue).isNowExecuting()){
                        logger.warn("Job " + jobKey + " is still executing !!!!");
                        WorkingStats.idlePlus();
                        stillExecuting=true;
                                                
                        for (int i=0; i<sybaseApiList.size(); i++){ 
                            sqlType type = sybaseApiList.get(i).getSqlType();
                            if (type == sqlType.SYSMON || type == sqlType.SYSMON_WITH_DISCOVERY || type==sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED){                             
                                if (sybaseApiList.get(i).getSysmonName().equalsIgnoreCase(sysmonApiList.get(sysmon_api_queue).getName())){
                                     WorkingStats.setLockForQueue(i);
                                }
                            }
                        }
                         
                        
                   } else {
                        
                       sysmonApiList.get(sysmon_api_queue).setIsNowExecuting();
                       
                        String query;
                        String value;
                        String thread; 
                        String threadpool; 
                        String engine; 
                        String loadtime; 
                        String node; 
                        String instanceid; 
                        String lockname; 
                        String loadprofile;
                        String logicalClusterName; 
                        String devicename;
                        String monitorconfig_name;
                        String monitorconfig_attribute;
                        String cache_name;

                      //  _sp_sysmon =  new SysmonEngine(sample_interval_ms);
                        _sp_sysmon = SysmonEngineList.get(sysmon_api_queue);
                        //BUG 20180118
                        WorkingStats.idlePlus();

                        String sybase_host= sysmonApiList.get(sysmon_api_queue).getSybaseHost();
                        int sybase_port=sysmonApiList.get(sysmon_api_queue).getSybasePort();

                        String sybase_user= sysmonApiList.get(sysmon_api_queue).getSybaseUser();
                        String sybase_password= sysmonApiList.get(sysmon_api_queue).getSybasePassword();
                      
                        int sybase_retry= sysmonApiList.get(sysmon_api_queue).getSybaseConnRetry();
                        int sybase_timeout= sysmonApiList.get(sysmon_api_queue).getSybaseConnTimeout();
                       
                        int sybase_poolID= sysmonApiList.get(sysmon_api_queue).getSybasePoolId();
                        String sybase_inf= sysmonApiList.get(sysmon_api_queue).getInterfaceType();
                        
                        String name= sysmonApiList.get(sysmon_api_queue).getName();
                        
                        for (int i=0; i<sybaseApiList.size(); i++){ 
                            sqlType type = sybaseApiList.get(i).getSqlType();

                            if (type == sqlType.SYSMON || type == sqlType.SYSMON_WITH_DISCOVERY || type==sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED){ 
                                String sysmon_name = sybaseApiList.get(i).getSysmonName();

                                if (sysmon_name.equalsIgnoreCase(name)){
                                     sybaseApiList.get(i).setIsNowExecuting();
                                }
                            }
                        }

                        _sp_sysmon.initSysmon(sysmonApiList.get(sysmon_api_queue).isUseMonTable(), sysmonApiList.get(sysmon_api_queue).getInterfaceType(), sysmonApiList.get(sysmon_api_queue).getSybasePoolId(),
                                sybase_host, sybase_port, sybase_user, sybase_password,sybase_timeout,sybase_retry);
                      
                        sysmonApiList.get(sysmon_api_queue).setTimeSpendingAndHits(_sp_sysmon.getExecuteTime(), _sp_sysmon.getNumberOfHits());
                                                
                        for (int i=0; i<sybaseApiList.size(); i++){ 
                            sqlType type = sybaseApiList.get(i).getSqlType();

                            if (type == sqlType.SYSMON || type == sqlType.SYSMON_WITH_DISCOVERY || type==sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED){ 
                            
                                if (sybaseApiList.get(i).getSysmonName().equalsIgnoreCase(name)){
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("Item to get data from sysmon " + sybaseApiList.get(i).getAlias() );
                                    }
                                    
                                    WorkingStats.setUnlockForQueue(i);
                                    sybaseApiList.get(i).clearReturnAndStateValue();

                                    if ( _sp_sysmon.isEngineError()){
                                        sybaseApiList.get(i).setReadyToSendToTrue();
                                        logger.error("DETECTED RETURN ERROR");
                                        
                                        if (null == type){
                                            sybaseApiList.get(i).addErrorReturnValue("Sysmon returned error");
                                            WorkingStats.errorCountPlus();
                                        } else  switch (type) {
                                            case SYSMON_WITH_DISCOVERY:
                                                {
                                                    //wysoki stopien ogolnosci
                                                    List<List<String>> maps = sybaseApiList.get(i).getDiscoveryMap();
                                                    
                                                    if (maps==null) {
                                                        logger.error("sysmonEngine for SYSMON_WITH_DISCOVERY returned error");
                                                        sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                        WorkingStats.errorCountPlus();
                                                    } else if (maps.isEmpty()){
                                                        logger.error("sysmonEngine for SYSMON_WITH_DISCOVERY returned error");
                                                        sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                        WorkingStats.errorCountPlus();
                                                    } else {  
                                                        //#BUG 20180529
                                                        for (int j=0; j<maps.size();j++){
                                                            logger.error("Add returned error for " + maps.get(j));
                                                            sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                            WorkingStats.errorCountPlus();
                                                        } 
                                                    }
                                                    break;
                                                }
                                            case SYSMON_WITH_DISCOVERY_EMBEDDED:
                                                {
                                                    //wysoki stopien ogolnosci 
                                                    List<List<String>> maps = sybaseApiList.get(i).getDiscoveryMap();
                                                    if (maps==null) {
                                                        logger.error("sysmonEngine for SYSMON_WITH_DISCOVERY_EMBEDDED returned error");
                                                        sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                        WorkingStats.errorCountPlus();
                                                    } else if (maps.isEmpty()){
                                                        logger.error("sysmonEngine for SYSMON_WITH_DISCOVERY_EMBEDDED returned error");
                                                        sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                        WorkingStats.errorCountPlus();
                                                    }   for (int j=0; j<maps.size();j++){
                                                        logger.error("Add returned error for " + maps.get(j));
                                                        sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                        WorkingStats.errorCountPlus();
                                                    }   break;
                                                }
                                            default:
                                                sybaseApiList.get(i).addErrorReturnValue("Sysmon returned error");
                                                WorkingStats.errorCountPlus();
                                                break;
                                        }
                                        
                                  
                                    } else {

                                            boolean processing=true;
                                            List<List<String>> maps = null;
                                            List<String> metaData = null;  
                                            List <String> key = null;
                                            boolean wasDiscoveryQuery=false;
                                            
                                            if (sybaseApiList.get(i).isDiscoveryParamsTypeFromFile()) key = sybaseApiList.get(i).getDiscoveryParamsType();

                                            if (type ==  sqlType.SYSMON_WITH_DISCOVERY ){
                                                  //wysoki stopien ogolnosci ;
                                                  sybaseApiList.get(i).setSybaseHost(sybase_host);
                                                  sybaseApiList.get(i).setSybasePort(Integer.toString(sybase_port));
                                                  sybaseApiList.get(i).setSybaseUser(sybase_user);
                                                  sybaseApiList.get(i).setSybasePassword(sybase_password);
                                                  sybaseApiList.get(i).setSybasePoolId(sybase_poolID);
                                                  sybaseApiList.get(i).setSybaseConnRetry(sybase_retry);
                                                  sybaseApiList.get(i).setSybaseConnTimeout(sybase_timeout);
                                                  sybaseApiList.get(i).setInterfaceType(sybase_inf);
                                                 
                                                           
                                                  if (sybaseApiList.get(i).mustDiscoveryNow()) {

                                                        String  discovery_query = sybaseApiList.get(i).getDiscoveryQuery();
                                                        SybaseCore sybaseCore = new SybaseCore();
                                                        maps = sybaseCore.getDiscoveryMap(i, discovery_query);
                                                        if (maps!=null ) sybaseApiList.get(i).setDiscoveryMapAndMetaData(maps);
                                                        sybaseCore=null;

                                                   } else {
                                                         maps = sybaseApiList.get(i).getDiscoveryMap();
                                                  }
                                                   if (maps!=null){
                                                       metaData = sybaseApiList.get(i).getDiscoveryMetaData();
                                                   } else {
                                                          logger.error("**** ERROR while getting metadata *****");   
                                                   }

                                            }  else  if (type ==  sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED ){
                                                  //wysoki stopien ogolnosci ;
                                                  sybaseApiList.get(i).setSybaseHost(sybase_host);
                                                  sybaseApiList.get(i).setSybasePort(Integer.toString(sybase_port));
                                                  sybaseApiList.get(i).setSybaseUser(sybase_user);
                                                  sybaseApiList.get(i).setSybasePassword(sybase_password);
                                                  sybaseApiList.get(i).setSybasePoolId(sybase_poolID);
                                                  sybaseApiList.get(i).setSybaseConnRetry(sybase_retry);
                                                  sybaseApiList.get(i).setSybaseConnTimeout(sybase_timeout);
                                                  sybaseApiList.get(i).setInterfaceType(sybase_inf);
                                                  
                                                  if (sybaseApiList.get(i).mustDiscoveryNow()) {

                                                        String  discovery_query = sybaseApiList.get(i).getDiscoveryQuery();
                                                        wasDiscoveryQuery=false;
                                                        
                                                        if (discovery_query.equalsIgnoreCase("sybaseThreadAndEngine")){
                                                            SybaseCore sybaseCore = new SybaseCore();
                                                            maps = sybaseCore.getSybaseThreadAndEngine(i);
                                                            sybaseCore=null;  
                                                            if (maps!=null ) sybaseApiList.get(i).setDiscoveryMapAndMetaData(maps);
                                                            wasDiscoveryQuery=true;
                                                        }
                                                   
                                                    

                                                  } else {
                                                       maps = sybaseApiList.get(i).getDiscoveryMap();
                                                  }
                                                  
                                                  if (maps!=null){
                                                       metaData = sybaseApiList.get(i).getDiscoveryMetaData();
                                                  } else {
                                                       logger.error("**** ERROR while getting metadata *****");   
                                                  }

                                            }

                                            int proc=0;

                                            sybaseApiList.get(i).clearReturnAndStateValue();
                                            long start_processing = System.currentTimeMillis();

                                            if (maps==null && type == sqlType.SYSMON_WITH_DISCOVERY  ) {
                                                 processing=false;
                                                 sybaseApiList.get(i).addErrorReturnValue("sp_sysmon map returned error ");
                                            } else  if (maps==null && type == sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED ) {
                                                 processing=false;
                                                 if (wasDiscoveryQuery) sybaseApiList.get(i).addErrorReturnValue("sp_sysmon map returned error ");
                                                 else sybaseApiList.get(i).addErrorReturnValue("sp_sysmon map returned error - wasNOTDiscoveryQuery");
                                            }


                                            while (processing){

                                                if (System.currentTimeMillis() - start_processing > 60000){
                                                    logger.error("****** PROCESSING TIMEOUT..... ******");
                                                    processing=false;
                                                    break;
                                                }

                                                if (type == sqlType.SYSMON_WITH_DISCOVERY || type == sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED){
                                                   if (proc>=maps.size()){ 
                                                        processing=false; 
                                                        logger.debug("Exit from processing....");    
                                                        if (proc==0) {
                                                            logger.error("Exit from processing, only metadata in maps....");
                                                        }
                                                        break;
                                                    }


                                                    List <String> keys_value = maps.get(proc);

                                                    if (keys_value.size()!=metaData.size()){
                                                        logger.error("Consist error metaData");
                                                        //#BUG 20170522
                                                        proc++;
                                                        continue;
                                                    }

                                                    if (sybaseApiList.get(i).isDiscoveryParamsTypeFromFile()) {

                                                        if (keys_value.size()!=key.size()){
                                                            logger.error("Consist error key");
                                                            //#BUG 20170522
                                                            proc++;
                                                            continue;
                                                        }
                                                    } 

                                                    for (int j=0; j<metaData.size();j++){
                                                        if (sybaseApiList.get(i).isDiscoveryParamsTypeFromFile()) {
                                                           sybaseApiList.get(i).setDatabaseProperty(key.get(j).toLowerCase(), keys_value.get(j));
                                                        } else {
                                                           sybaseApiList.get(i).setDatabaseProperty(metaData.get(j).toLowerCase(), keys_value.get(j));
                                                        }
                                                    }

                                                }

                                                query = sybaseApiList.get(i).getSqlQuery();
                                                value="0.0";
                                                threadpool = sybaseApiList.get(i).getDatabaseProperty("threadpool", "default"); 
                                                thread = sybaseApiList.get(i).getDatabaseProperty("thread", "default"); 
                                                engine = sybaseApiList.get(i).getDatabaseProperty("engine", "default"); 
                                                loadtime = sybaseApiList.get(i).getDatabaseProperty("loadtime", "1min").replace("min", ""); 
                                                node = sybaseApiList.get(i).getDatabaseProperty("node", "default"); 
                                                instanceid = sybaseApiList.get(i).getDatabaseProperty("instanceid", "default"); 
                                                lockname = sybaseApiList.get(i).getDatabaseProperty("lockname", "default"); 
                                                loadprofile = sybaseApiList.get(i).getDatabaseProperty("loadprofile", "default");
                                                logicalClusterName = sybaseApiList.get(i).getDatabaseProperty("logicalclustername", "default"); 
                                                devicename = sybaseApiList.get(i).getDatabaseProperty("device", "default");
                                                monitorconfig_name = sybaseApiList.get(i).getDatabaseProperty("monitorconfig_name", "default");
                                                monitorconfig_attribute  = sybaseApiList.get(i).getDatabaseProperty("monitorconfig_attribute", "default");       
                                                cache_name = sybaseApiList.get(i).getDatabaseProperty("cache_name", "default data cache");  
                                                        
                                                int _node =0;
                                                
                                                //po nowemu jak cos
                                                if (!instanceid.equalsIgnoreCase("default")){
                                                    node=instanceid;
                                                }


                                                if (_sp_sysmon.isEngineError()){
                                                    logger.warn("SP_SYSMON returned ERROR");
                                                    sybaseApiList.get(i).setReadyToSendToFalse();
                                                    sybaseApiList.get(i).addErrorReturnValue("SysmonEngine returned error");
                                                    //#BUG 20170519
                                                     if (type == sqlType.SYSMON ||type == sqlType.SYSMON_WITH_DISCOVERY ||type == sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED ) processing=false;
                                                     else {
                                                         proc++;
                                                     }
                                                    continue;
                                                }


                                                if (logger.isDebugEnabled()) logger.debug("Engine " + engine + ":node " + node + " query " + query + " devicename " + devicename);

                                                int _engine = 0;
                                                int _loadtime = 1;
                                                
                                                try {
                                                    _loadtime=Integer.parseInt(loadtime);
                                                } catch (Exception ii){}
                                                
                                                if (_loadtime!=1 && _loadtime!=5 && _loadtime!=15) _loadtime=1;                                                
                                              
                                                if (engine.equalsIgnoreCase("default")){
                                                    if (query.toLowerCase().contains("engine")) {
                                                        logger.warn("Engine default set to ALL");
                                                    }
                                                    engine = "all";
                                                    _engine = -1;
                                                } else if (engine.equalsIgnoreCase("all")){
                                                   _engine = -1;
                                                } else {
                                                    try {
                                                        _engine = Integer.parseInt(engine);
                                                    } catch (NumberFormatException e){}

                                                }

                                                if (!node.equalsIgnoreCase("default")){
                                                   // logger.error("SET node DEPRECATED!!!!!!");
                                                    try {
                                                        _node = Integer.parseInt(node);
                                                        //_sp_sysmon.setInstanceIdManualy(_node);
                                                    } catch (NumberFormatException e){}
                                                } 
                

                                                if (query.equalsIgnoreCase("EngineUtilIOBusy")) { 
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){  
                                                             try {
                                                                       value = df.format(_sp_sysmon.getEngineUtilIOBusy());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                       logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getEngineUtilIOBusy(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }


                                                         }
                                                } else if (query.equalsIgnoreCase("EngineUtilSystemBusy") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){  
                                                             try {
                                                                       value = df.format(_sp_sysmon.getEngineUtilSystemBusy());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                       logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getEngineUtilSystemBusy(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }


                                                         }
                                                 } else if (query.equalsIgnoreCase("EngineUtilUserBusy") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){  
                                                             try {
                                                                       value = df.format(_sp_sysmon.getEngineUtilUserBusy());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                       logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getEngineUtilUserBusy(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   //e.printStackTrace();
                                                                   logger.error(query + e);
                                                                   WorkingStats.errorCountPlus();
                                                               }


                                                         }
                                                 }  else if (query.equalsIgnoreCase("EngineUtilUserPlusSystemBusy") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){  
                                                             try {
                                                                       value = df.format(_sp_sysmon.getEngineUtilUserPlusSystemBusy());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                       logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getEngineUtilUserPlusSystemBusy(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }


                                                         }
                                                 } else if (query.equalsIgnoreCase("EngineUtilIdleBusy") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){  
                                                             try {
                                                                       value = df.format(_sp_sysmon.getEngineUtilIdleBusy());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                        logger.error(e);
                                                                        WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getEngineUtilIdleBusy(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }


                                                         } 
                                                } else if (query.equalsIgnoreCase("EngineUtilCPUBusy") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){ 
                                                             try {
                                                                       value = df.format(_sp_sysmon.getEngineUtilCPUBusy());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                      logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getEngineUtilCPUBusy(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }

                                                         }

                                                } else if (query.equalsIgnoreCase("AvgRunnableTasks") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){ 
                                                             try {
                                                                 switch (_loadtime) {
                                                                     case 1:
                                                                         value = df.format(_sp_sysmon.getAvgRunnableTasks1minGlobalQueue());
                                                                         break;
                                                                     case 5:
                                                                         value = df.format(_sp_sysmon.getAvgRunnableTasks5minGlobalQueue());
                                                                         break;
                                                                     default:
                                                                         value = df.format(_sp_sysmon.getAvgRunnableTasks15minGlobalQueue());
                                                                         break;
                                                                 }
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                      logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                 switch (_loadtime) {
                                                                     case 1:
                                                                         value = df.format(_sp_sysmon.getAvgRunnableTasks1minEngine(_engine));
                                                                         break;
                                                                     case 5:
                                                                         value = df.format(_sp_sysmon.getAvgRunnableTasks5minEngine(_engine));
                                                                         break;
                                                                     default:
                                                                         value = df.format(_sp_sysmon.getAvgRunnableTasks15minEngine(_engine));
                                                                         break;
                                                                  }
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }

                                                         }

                                                }  else if (query.equalsIgnoreCase("AvgRunnableTasksAvgEngine") ){
                                                         WorkingStats.testsCountPlus();
                                                        
                                                         try {
                                                             switch (_loadtime) {
                                                                 case 1:
                                                                     value = df.format(_sp_sysmon.getAvgRunnableTasks1minAvgEngine());
                                                                     break;
                                                                 case 5:
                                                                     value = df.format(_sp_sysmon.getAvgRunnableTasks5minAvgEngine());
                                                                     break;
                                                                 default:
                                                                     value = df.format(_sp_sysmon.getAvgRunnableTasks15minAvgEngine());
                                                                     break;
                                                             }
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                           } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                           }

                                                         

                                                }  else if (query.equalsIgnoreCase("CPUYields") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){ 
                                                             try {
                                                                       value = df.format(_sp_sysmon.getCPUYields());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                      logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getCPUYields(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }

                                                         }

                                                } else if (query.equalsIgnoreCase("CPUYieldsFullSleeps") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){ 
                                                             try {
                                                                       value = df.format(_sp_sysmon.getCPUYieldsFullSleeps());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                      logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getCPUYieldsFullSleeps(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }

                                                         }

                                                } else if (query.equalsIgnoreCase("CPUYieldsInterruptedSleeps") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){ 
                                                             try {
                                                                       
                                                                       value = df.format(_sp_sysmon.getCPUYieldsInterruptedSleeps());
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                      logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getCPUYieldsInterruptedSleeps(_engine));
                                                                   //System.out.println(_engine + "-" + value);
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }

                                                         }

                                                } else if (query.equalsIgnoreCase("CPUYieldsFullSleeps_prc") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine > -1){ 
                                                                try {
                                                                   value = df.format(_sp_sysmon.getCPUYieldsFullSleeps_prc(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }
                                                         }

                                                } else if (query.equalsIgnoreCase("CPUYieldsInterruptedSleeps_prc") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine > -1){ 
                                                               try {
                                                                       value = df.format(_sp_sysmon.getCPUYieldsInterruptedSleeps_prc(_engine));
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                      logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }
                                                        }

                                                } else if (query.equalsIgnoreCase("CPUYields_prc") ){
                                                         WorkingStats.testsCountPlus();
                                                        if (_engine > -1){ 
                                                               try {
                                                                   value = df.format(_sp_sysmon.getCPUYields_prc(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }

                                                         }

                                                }  else if (query.equalsIgnoreCase("WorkloadsConnectionRaw") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                value = _sp_sysmon.getWorkloadsConnectionRaw(_node);
                                                                sybaseApiList.get(i).addNormalReturnValue(value);
                                                                sybaseApiList.get(i).setReadyToSendToTrue();
                                                                WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                logger.error(e);
                                                                WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("WorkloadsCpuRaw") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                value = _sp_sysmon.getWorkloadsCpuRaw(_node);
                                                                sybaseApiList.get(i).addNormalReturnValue(value);
                                                                sybaseApiList.get(i).setReadyToSendToTrue();
                                                                WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                logger.error(e);
                                                                WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("WorkloadsRunQueueRaw") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                value = _sp_sysmon.getWorkloadsRunQueueRaw(_node);
                                                                sybaseApiList.get(i).addNormalReturnValue(value);
                                                                sybaseApiList.get(i).setReadyToSendToTrue();
                                                                WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                logger.error(e);
                                                                WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("WorkloadsIoLoadRaw") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                value = _sp_sysmon.getWorkloadsIoLoadRaw(_node);
                                                                sybaseApiList.get(i).addNormalReturnValue(value);
                                                                sybaseApiList.get(i).setReadyToSendToTrue();
                                                                WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                logger.error(e);
                                                                WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("WorkloadsEngineRaw") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                value = _sp_sysmon.getWorkloadsEngineRaw(_node);
                                                                sybaseApiList.get(i).addNormalReturnValue(value);
                                                                sybaseApiList.get(i).setReadyToSendToTrue();
                                                                WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                logger.error(e);
                                                                WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("ActiveInstances") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                   value = Integer.toString(_sp_sysmon.getActiveInstances());
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                  sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                 }  else if (query.equalsIgnoreCase("NodeStatus") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                   value = _sp_sysmon.getNodeStatus(_node);
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("NodeUsers") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                                value = Integer.toString(_sp_sysmon.getNodeUsers(_node));
                                                                sybaseApiList.get(i).addNormalReturnValue(value);
                                                                sybaseApiList.get(i).setReadyToSendToTrue();
                                                                WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                logger.error(e);
                                                                WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("ClusterLoadScore") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getClusterLoadScore(_node);      
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("ProfileLoadScore") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getProfileLoadScore(loadprofile, _node);
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }   else if (query.equalsIgnoreCase("LogicalClusterActiveConnetions") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getLogicalClusterActiveConnetions(logicalClusterName,_node);
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("TotalRequestedDiskIOs") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getTotalRequestedDiskIOs();
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("TotalCompletedAsynchronousIOs") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getTotalCompletedAsynchronousIOs();
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("TotalCompletedSynchronousIOs") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getTotalCompletedSynchronousIOs();
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("TotalCompletedIOs") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getTotalCompleteIOs();
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("TotalNetworkIORequests") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getTotalNetworkIORequests();
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                              sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }   else if (query.equalsIgnoreCase("NumberOfLocksType") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = Integer.toString(_sp_sysmon.getNumberOfLocksType(lockname));
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                               sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("ThreadPoolThreadSystemUtilization") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getThreadPoolThreadSystemUtilization(threadpool);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               }  else if (query.equalsIgnoreCase("ThreadPoolThreadUserUtilization") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getThreadPoolThreadUserUtilization(threadpool);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               } else if (query.equalsIgnoreCase("ThreadPoolThreadIdleUtilization") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getThreadPoolThreadIdleUtilization(threadpool);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               } else if (query.equalsIgnoreCase("ThreadPoolEngineUtilIOBusy") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = df.format(_sp_sysmon.getThreadPoolEngineUtilIOBusy(threadpool));
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               } else if (query.equalsIgnoreCase("ThreadPoolEngineUtilUserBusy") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = df.format(_sp_sysmon.getThreadPoolEngineUtilUserBusy(threadpool));
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               } else if (query.equalsIgnoreCase("ThreadPoolEngineUtilSystemBusy") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = df.format(_sp_sysmon.getThreadPoolEngineUtilSystemBusy(threadpool));
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               }  else if (query.equalsIgnoreCase("ThreadPoolEngineUtilIdleBusy") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = df.format(_sp_sysmon.getThreadPoolEngineUtilIdleBusy(threadpool));
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               }  else if (query.equalsIgnoreCase("ThreadSystemUtilization") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = _sp_sysmon.getThreadSystemUtilization(thread, engine);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("ThreadUserUtilization") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getThreadUserUtilization(thread, engine);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("ThreadIdleUtilization") ){
                                                         WorkingStats.testsCountPlus();
                                                      
                                                         try {
                                                               value =  _sp_sysmon.getThreadIdle(thread, engine);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                              
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("ThreadIdleUtilizationAvg") ){
                                                         WorkingStats.testsCountPlus();
                                                      
                                                         try {
                                                               value =  _sp_sysmon.getThreadIdleUtilizationAvg();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                              
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("ThreadUserUtilizationAvg") ){
                                                         WorkingStats.testsCountPlus();
                                                      
                                                         try {
                                                               value =  _sp_sysmon.getThreadUserUtilizationAvg();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                              
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }   else if (query.equalsIgnoreCase("ThreadSystemUtilizationAvg") ){
                                                         WorkingStats.testsCountPlus();
                                                      
                                                         try {
                                                               value =  _sp_sysmon.getThreadSystemUtilizationAvg();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                              
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("MajorFaults") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getMajorFaults(thread, engine);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("TotalMajorFaults") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getTotalMajorFaults();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("MinorFaults") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getMinorFaults(thread, engine);
                                                                if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("TotalMinorFaults") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getTotalMinorFaults();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("VoluntaryCtxSwitches") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getVoluntaryCtxSwitches(thread, engine);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("TotalVoluntaryCtxSwitches") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getTotalVoluntaryCtxSwitches();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("NonVoluntaryCtxSwitches") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getNonVoluntaryCtxSwitches(thread, engine);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("TotalNonVoluntaryCtxSwitches") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getTotalNonVoluntaryCtxSwitches();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("ThreadCpuUnitsConsuming") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getThreadCpuUnitsConsuming();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("ProcedureCacheHitRatio") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getProcedureCacheHitRatio();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("DataCacheHitRatio") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getDataCacheHitRatio(cache_name);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("DataInMemoryCacheHitRatio") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getDataInMemoryCacheHitRatio(cache_name);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("DeviceIOTime") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getIOTime(devicename);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("DeviceIOOperations") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getIOOperations(devicename);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("DeviceTotalIOs") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getDeviceTotalIOs(devicename);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("DeviceWrites") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getDeviceWrites(devicename);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               }  else if (query.equalsIgnoreCase("DeviceReadsNonAPF") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getDeviceReadsNonAPF(devicename);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               }  else if (query.equalsIgnoreCase("DeviceReadsAPF") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getDeviceReadsAPF(devicename);
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                               } else if (query.equalsIgnoreCase("NetworkIOBytesReceivedMB") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getNetworkIOBytesReceivedMB();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }   else if (query.equalsIgnoreCase("NetworkIOBytesSentMB")){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getNetworkIOBytesSentMB();
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("MaxOutstandingAIOsServer")){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  Long.toString(_sp_sysmon.getMax_outstanding_AIOs_server());
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("MaxOutstandingAIOsEngine") ){
                                                         WorkingStats.testsCountPlus();
                                                         if (_engine == -1){  
                                                             try {
                                                                       value = Long.toString(_sp_sysmon.getMax_outstanding_AIOs_engine(_engine));
                                                                       sybaseApiList.get(i).addNormalReturnValue(value);
                                                                       sybaseApiList.get(i).setReadyToSendToTrue();
                                                                       WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                       logger.error(e);
                                                                       WorkingStats.errorCountPlus();
                                                               }

                                                         } else {
                                                               try {
                                                                   value = df.format(_sp_sysmon.getMax_outstanding_AIOs_engine(_engine));
                                                                   sybaseApiList.get(i).addNormalReturnValue(value);
                                                                   sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.okCountPlus();
                                                               } catch (Exception e){
                                                                   logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                               }


                                                         }
                                                 } else if (query.equalsIgnoreCase("DelayedByDiskIOStructures")){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  Long.toString(_sp_sysmon.getDelayedByDiskIOStructures());
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("DelayedByServerConfigLimit")){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  Long.toString(_sp_sysmon.getDelayedByServerConfigLimit());
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("DelayedByOperatingSystemLimit")){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  Long.toString(_sp_sysmon.getDelayedByOperatingSystemLimit());
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("DelayedByEngineConfigLimit")){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  Long.toString(_sp_sysmon.getDelayedByEngineConfigLimit());
                                                               if (_sp_sysmon.isErrorInTest()){
                                                                    sybaseApiList.get(i).addErrorReturnValue("Error in test " + value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                   WorkingStats.errorCountPlus();
                                                               } else {
                                                                    sybaseApiList.get(i).addNormalReturnValue(value);
                                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                                    WorkingStats.okCountPlus();
                                                               }
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else if (query.equalsIgnoreCase("BufImdbPrivateBufferGrab") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value =  _sp_sysmon.getBufImdbPrivateBufferGrab();
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                              sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }
                                                }   else if (query.equalsIgnoreCase("sp_monitorconfig") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                             if (monitorconfig_attribute.equalsIgnoreCase("Num_free")){
                                                                 value =  _sp_sysmon.getSp_monitorconfigNumFree(monitorconfig_name);
                                                             } else  if (monitorconfig_attribute.equalsIgnoreCase("Num_active")){
                                                                 value =  _sp_sysmon.getSp_monitorconfigNumActive(monitorconfig_name);
                                                             }  else  if (monitorconfig_attribute.equalsIgnoreCase("Pct_Act")){
                                                                 value =  _sp_sysmon.getSp_monitorconfigPctAct(monitorconfig_name);
                                                             } else  if (monitorconfig_attribute.equalsIgnoreCase("Max_Used")){
                                                                 value =  _sp_sysmon.getSp_monitorconfigMaxUsed(monitorconfig_name);
                                                             } else  if (monitorconfig_attribute.equalsIgnoreCase("ReuseCnt")){
                                                                 value =  _sp_sysmon.getSp_monitorconfigReuseCnt(monitorconfig_name);
                                                             }
                                                             
                                                              sybaseApiList.get(i).addNormalReturnValue(value);
                                                              sybaseApiList.get(i).setReadyToSendToTrue();
                                                              WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }
                                                } else if (query.equalsIgnoreCase("ExecuteTime") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = Long.toString(_sp_sysmon.getExecuteTime());
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                              sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                }  else if (query.equalsIgnoreCase("CalculateTime") ){
                                                         WorkingStats.testsCountPlus();

                                                         try {
                                                               value = Long.toString(_sp_sysmon.getCalculateTime());
                                                               sybaseApiList.get(i).addNormalReturnValue(value);
                                                              sybaseApiList.get(i).setReadyToSendToTrue();
                                                               WorkingStats.okCountPlus();
                                                          } catch (Exception e){
                                                                  logger.error(e);
                                                                   WorkingStats.errorCountPlus();
                                                          }

                                                } else {
                                                    WorkingStats.errorCountPlus();
                                                    sybaseApiList.get(i).addErrorReturnValue("0.0");
                                                    sybaseApiList.get(i).setReadyToSendToTrue();
                                                }

                                               if (type == sqlType.SYSMON ) processing=false;
                                               else {
                                                   proc++;
                                               }

                                           } //processing
                                    
                                   } //end if not error
                                   
                                   sybaseApiList.get(i).setIsNowExecutingToFalse();
                                   sybaseApiList.get(i).setIsNowRefreshingToFalse();

                                } //end name

                            }


                        }  
                        stillExecuting=false;
                       // Runtime.getRuntime().gc();
                   }
          
            }  catch (Exception e) {
                logger.error("--- Error in job ! ----");
                
                Thread.currentThread().interrupt();
             
                JobExecutionException e2 = new JobExecutionException(e);
        	// this job will refire immediately
        	//e2.refireImmediately();
                throw e2;  
          } finally {
                   /*if (_sp_sysmon!=null) _sp_sysmon.destroySysmon();
                   
                   //++++++ 04022019
                   _sp_sysmon = null;*/
                   
                    if (_sp_sysmon!=null) _sp_sysmon.clearSysmon();
                   
                   if (!stillExecuting){
                       sysmonApiList.get(sysmon_api_queue).setIsNowExecutingToFalse();
                       sysmonApiList.get(sysmon_api_queue).setIsNowRefreshingToFalse();
                   }  else {
                      WorkingStats.idlePlus();
                   }
                  
                   if (isJobInterrupted) {
                    logger.warn("Job " + jobKey + " did not complete");
                  } else {
                    logger.debug("Job " + jobKey + " completed at " + new Date());
                  }
            } 
        }
        
 
        
      @Override
     // this method is called by the scheduler
      public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Job " + jobKey + "  -- INTERRUPTING --");
        isJobInterrupted = true;
        if (thisThread != null) {
          // this call causes the ClosedByInterruptException to happen
          thisThread.interrupt(); 
        }
      }

 
}