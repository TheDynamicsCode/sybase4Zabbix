/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;



import static biz.szydlowski.sybase4zabbix.WorkingObjects.sybaseApiList;
import biz.szydlowski.utils.WorkingStats;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
 * @author szydlowskidom
 */
public class SybaseQuartzJob implements InterruptableJob {
    
   
     static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.SybaseQuartzJob");
   
    private volatile Thread  thisThread;    
    private JobKey   jobKey   = null; 
    private volatile boolean isJobInterrupted = false;

    String value = "init";
    int queue = 0;
 
  
    public  SybaseQuartzJob(){ 
    }

    
    @Override
    public void execute(JobExecutionContext jeContext) throws JobExecutionException {

        thisThread = Thread.currentThread();
       // logger.info("Thread name of the current job: " + thisThread.getName());

        jobKey = jeContext.getJobDetail().getKey();
        logger.debug("Job " + jobKey + " executing at " + new Date());
        boolean stillExecuting=false;
        
        try {

                JobDataMap jdMap = jeContext.getJobDetail().getJobDataMap();
                
                SybaseCore _SybaseCore = new SybaseCore();
                queue= Integer.parseInt(jdMap.get("queue").toString());
                
                if (queue>sybaseApiList.size()){
                    logger.fatal("sybaseApiList error");
                }
                if (!sybaseApiList.get(queue).isActiveMode()){
                     WorkingStats.idlePlus();
                     WorkingStats.setUnlockForQueue(queue);
                     
                } else  if (sybaseApiList.get(queue).isNowExecuting()){
                    logger.warn("Job " + jobKey + " is still executing !!!!");
                    //BUG 20180103S
                    WorkingStats.idlePlus();
                    WorkingStats.setLockForQueue(queue);
                    stillExecuting=true; 
                    
                } else {
                    WorkingStats.setUnlockForQueue(queue);
                    
                     //#BUG 20170606
                    sybaseApiList.get(queue).setIsNowExecuting();
                    sybaseApiList.get(queue).clearReturnAndStateValue();
                  
                    sqlType sql_type = sybaseApiList.get(queue).getSqlType();
                    String  sql_query = sybaseApiList.get(queue).getSqlQuery();  
                                    
                    switch (sql_type) {
                    
                        case DISCOVERY_EMBEDDED:
                            if (sql_query.equalsIgnoreCase("rs_threads")){
                                List<List<String>> discovery = _SybaseCore.getRepAllThreadsMap(queue);
                               sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                
                                if (_SybaseCore.isError()){
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(new ArrayList<List<String>>());
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                    WorkingStats.errorCountPlus();
                                } else {
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(discovery);
                                    sybaseApiList.get(queue).setReadyToSendToTrue();
                                    WorkingStats.okCountPlus();
                                }
                            }  else if (sql_query.equalsIgnoreCase("rs_threads_lite")){
                                List<List<String>> discovery = _SybaseCore.getRepLiteThreadsMap(queue);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                 
                                if (_SybaseCore.isError()){
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(new ArrayList<>());
                                    sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                    WorkingStats.errorCountPlus();
                                } else {
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(discovery);
                                    sybaseApiList.get(queue).setReadyToSendToTrue();
                                    WorkingStats.okCountPlus();
                                }
                            } else if (sql_query.equalsIgnoreCase("rs_queues")){
                                List<List<String>> discovery = _SybaseCore.getRepAllQueuesMap(queue);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                 
                                if (_SybaseCore.isError()){
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(new ArrayList<>());
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                    WorkingStats.errorCountPlus();
                                } else {
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(discovery);
                                    sybaseApiList.get(queue).setReadyToSendToTrue();
                                    WorkingStats.okCountPlus();
                                }
                                
                            } else if (sql_query.equalsIgnoreCase("sybaseThreadAndEngine")){
                               // System.out.println("sybaseThreadAndEngine");
                                List<List<String>> discovery = _SybaseCore.getSybaseThreadAndEngine(queue);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                 
                                if (_SybaseCore.isError()){
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(new ArrayList<>());
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                    WorkingStats.errorCountPlus();
                                } else {
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(discovery);
                                    sybaseApiList.get(queue).setReadyToSendToTrue();
                                    WorkingStats.okCountPlus();
                                }
                                
                            }  
                            break;
                        
                        case DISCOVERY_STATIC: //tutaj jest zawsze discovery !!!
                           sybaseApiList.get(queue).setDiscoveryMapAndMetaData(sybaseApiList.get(queue).getSqlQuery()); 
                           sybaseApiList.get(queue).setReadyToSendToTrue();
                           WorkingStats.okCountPlus();
                       
                        case DISCOVERY_QUERY: 
                            if (!sybaseApiList.get(queue).isDiscoveryMapFromFile()){
                            
                                List<List<String>> discovery = _SybaseCore.getDiscoveryMap(queue, sql_query);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                
                                if (_SybaseCore.isError()){
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(new ArrayList<>());
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                    WorkingStats.errorCountPlus();
                                } else {
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(discovery);
                                    sybaseApiList.get(queue).setReadyToSendToTrue();
                                    WorkingStats.okCountPlus();
                                } 
                            } else {
                                sybaseApiList.get(queue).setReadyToSendToTrue();
                            }
                            
                            break;       
                    
                        case EMBEDDED_WITH_DISCOVERY:
                            {
                                // logger.debug("EMBEDDED_WITH_DISCOVERY");
                                //wysoki stopien ogolnosci
                                List<List<String>> maps = null;
                                // jesli jest discovery z pliku to pominie
                                if (sybaseApiList.get(queue).mustDiscoveryNow()) {
                                    
                                    logger.debug("** DISCOVERY *** " + queue);
                                                                  
                                    SybaseCore sybaseCore = new SybaseCore();
                                    maps = sybaseCore.getDiscoveryMap(queue, sybaseApiList.get(queue).getDiscoveryQuery()); 
                                    sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                    sybaseCore = null;
                                    
                                    if (maps!=null)  sybaseApiList.get(queue).setDiscoveryMapAndMetaData(maps);
                                } else {
                                    maps = sybaseApiList.get(queue).getDiscoveryMap();
                                }   
                                
                                if (maps!=null){
                                    //List<String> metaData=sybaseApiList.get(queue).getDiscoveryMetaData();
                                    
                                    for(int i=0; i<maps.size();i++) {
                                        
                                      //  List <String> keys_value = maps.get(i);
                                        if (maps.get(i).size()!=sybaseApiList.get(queue).getDiscoveryMetaData().size()){
                                            logger.error("Consist error metaData");
                                            continue;
                                        }
                                        
                                        for (int j=0; j<sybaseApiList.get(queue).getDiscoveryMetaData().size();j++){
                                            sybaseApiList.get(queue).setDatabaseProperty(sybaseApiList.get(queue).getDiscoveryMetaData().get(j).toLowerCase(), maps.get(i).get(j));
                                        }
                                        
                                        if (i==0) {
                                            //logger.debug("BEGIN M_EMBEDDED");
                                            value  = _SybaseCore.doAndReturnData(queue, true, false); //open=true; close=false
                                            sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                        } else if (i==maps.size()-1){
                                            //logger.debug("END M_EMBEDDED");
                                            value  = _SybaseCore.doAndReturnData(queue, false, true); //close=true
                                             sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                        }
                                        else  {
                                            value  = _SybaseCore.doAndReturnData(queue, false, false);
                                            sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                                            
                                        }
                                        
                                        WorkingStats.testsCountPlus();
                                        
                                        if (_SybaseCore.isError()){
                                            sybaseApiList.get(queue).addErrorReturnValue(_SybaseCore.getErrorDescription());
                                            logger.error("Sybase core returned error.");
                                            WorkingStats.errorCountPlus();
                                        } else {
                                            sybaseApiList.get(queue).addNormalReturnValue(value);
                                            WorkingStats.okCountPlus();
                                        }
                                        
                                    } // cala mapa
                                    sybaseApiList.get(queue).setReadyToSendToTrue();
                                } else {
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    sybaseApiList.get(queue).addErrorReturnValue("Sybase core  map returned error ");
                                    logger.error("Discovery maps is null");
                                }       
                                break;
                            }
                        case EMBEDDED_WITH_DISCOVERY_EMBEDDED:
                            List<List<String>> maps_with_values = null;
                            if (sybaseApiList.get(queue).getSqlQuery().equalsIgnoreCase("all_rs_threads")) {
                                maps_with_values = _SybaseCore.getRepAllThreadsValue(queue);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                            }  else if (sybaseApiList.get(queue).getSqlQuery().equalsIgnoreCase("all_rs_threads_lite")) {
                                maps_with_values = _SybaseCore.getRepLiteThreadsValue(queue);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                            }  else if (sybaseApiList.get(queue).getSqlQuery().equalsIgnoreCase("all_rs_queues")) {
                                maps_with_values = _SybaseCore.getRepAllQueuesValues(queue);
                                 sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                            } else {
                                logger.error("Unknown option " + sybaseApiList.get(queue).getSqlQuery());
                            }
                            if (_SybaseCore.isError()){
                                logger.error("Sybase core returned error.");
                                sybaseApiList.get(queue).addErrorReturnValue("Sybase core returned error...");
                                sybaseApiList.get(queue).setReadyToSendToFalse();
                                WorkingStats.errorCountPlus();
                            } else {
                                if (maps_with_values!=null){
                                    maps_with_values.get(0).remove(maps_with_values.get(0).size()-1); //remove values from metadata
                                    for (int k=1; k<maps_with_values.size();k++){
                                        int index = maps_with_values.get(k).size()-1;
                                        String value = maps_with_values.get(k).get(index);
                                        maps_with_values.get(k).remove(index); //remove values
                                        sybaseApiList.get(queue).addNormalReturnValue(value);
                                        sybaseApiList.get(queue).setReadyToSendToTrue();
                                        WorkingStats.okCountPlus();
                                    }
                                    sybaseApiList.get(queue).setDiscoveryMapAndMetaData(maps_with_values);
                                }  else {
                                    sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error ");
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    logger.error("Discovery maps is null");
                                }
                            }
                            break;
                        case PURE_WITH_MULTIPLE:
                        case PREDEFINED_WITH_MULTIPLE:
                            {
                                logger.debug("**_WITH_MULTIPLE");
                                WorkingStats.testsCountPlus();
                                //index out of
                                String _query = sybaseApiList.get(queue).getSqlQuery();
                               /* if (_SybaseCore.isError()){
                                    logger.error("Sybase core returned error.");
                                    sybaseApiList.get(queue).addErrorReturnValue("sybase core returned error ");
                                    sybaseApiList.get(queue).setReadyToSendToFalse(); //done with errors
                                    WorkingStats.errorCountPlus();
                                } else {*/
                                    
                                List<List<String>> maps  = _SybaseCore.getDiscoveryMap(queue, _query);
                                sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                              

                                if (_SybaseCore.isError()){
                                    logger.error("Sybase core returned error.");
                                    sybaseApiList.get(queue).addErrorReturnValue("sybase core returned error ");
                                    sybaseApiList.get(queue).setReadyToSendToFalse(); //done with errors
                                    WorkingStats.errorCountPlus();
                                } else {
                                    if (maps!=null) { 
                                        if (sybaseApiList.get(queue).isComplementary()){
                                            if (sybaseApiList.get(queue).mustComplementaryNow()) {

                                               logger.info("** DISCOVERY Complementary *** " + queue);                                                
                                                
                                               logger.info("Complementary " + sybaseApiList.get(queue).getComplementary_query());
                                            
                                                List<List<String>> complement  = _SybaseCore.getDiscoveryMap(queue, sybaseApiList.get(queue).getComplementary_query());
                                                sybaseApiList.get(queue).setComplementaryMap(complement);

                                            }   else  if (sybaseApiList.get(queue).getComplementaryMap().isEmpty()){
                                                    if (sybaseApiList.get(queue).isComplementaryMapFromFile()){
                                                        logger.error("Empty complementary read from file");
                                                    } else {
                                                        logger.info("Complementary is empty ");
                                                        List<List<String>> complement  = _SybaseCore.getDiscoveryMap(queue, sybaseApiList.get(queue).getComplementary_query());
                                                        sybaseApiList.get(queue).setComplementaryMap(complement);
                                                    }
                                            } 
                                                                                   
                                            if (sybaseApiList.get(queue).getComplementaryMap()==null){
                                                logger.error("sybaseApiList.get(queue).getComplementMap() is null");
                                            } else {
                                            
                                                int valcol=-1;

                                                for (int j=0; j<maps.get(0).size(); j++){ //get metadata
                                                      if ( maps.get(0).get(j).equalsIgnoreCase("VALUE")){
                                                         valcol=j;
                                                         break;
                                                      }
                                                }
                                                
                                                if (valcol<0) logger.warn("valcol not found!!!");

                                                if (sybaseApiList.get(queue).getComplementaryMap().get(0).size()!=maps.get(0).size()){
                                                    logger.error("Error x55 complementary.get(0).size()!=maps.get(0).size()");
                                                } else {
                                                        for (int c=1; c < sybaseApiList.get(queue).getComplementaryMap().size();c++){ //0==metadata 
                                                            boolean bfound=false;
                                                            for (int j=1; j < maps.size();j++){ //0==metadata 
                                                               int found=0;
                                                               for  (int k=0; k< maps.get(j).size(); k++){ //kolumny
                                                                   if (k!=valcol){
                                                                       //logger.info("compare " + maps.get(j).get(k));
                                                                       //logger.info(sybaseApiList.get(queue).getComplementaryMap().get(c).get(k));
                                                                       if (maps.get(j).get(k).equals(sybaseApiList.get(queue).getComplementaryMap().get(c).get(k))) found++;
                                                                   }
                                                               }
                                                                if (found==sybaseApiList.get(queue).getComplementaryMap().get(c).size()-1){
                                                                     bfound=true;
                                                                } 
                                                            }
                                                            if (!bfound){
                                                                logger.debug("Add complementary " + sybaseApiList.get(queue).getComplementaryMap().get(c).toString());
                                                                maps.add(sybaseApiList.get(queue).getComplementaryMap().get(c));
                                                            }
                                                        }

                                                }
                                            }
                                          
                                        }
                                        
                                        
                                        sybaseApiList.get(queue).setDiscoveryMapAndMetaData(maps);
                                        sybaseApiList.get(queue).setReadyToSendToTrue();
                                        WorkingStats.okCountPlus();
                                    }  else {
                                        sybaseApiList.get(queue).setReadyToSendToFalse();
                                        sybaseApiList.get(queue).addErrorReturnValue("DiscoveryMap map returned error");
                                        WorkingStats.errorCountPlus();
                                    }
                                }
                                    
                                    
                                //}      
                                break;
                            }   
                        
                        case PURE:
                            value  = _SybaseCore.doAndReturnData(queue, true, true);
                            sybaseApiList.get(queue).setTimeSpending(_SybaseCore.getExecuteTime());
                             WorkingStats.testsCountPlus();
                            if (_SybaseCore.isError()){
                                logger.error("Sybase core returned error.");
                                sybaseApiList.get(queue).addErrorReturnValue(_SybaseCore.getErrorDescription());
                                sybaseApiList.get(queue).setReadyToSendToTrue(); //done with errors
                                WorkingStats.errorCountPlus();
                            } else {
                                sybaseApiList.get(queue).addNormalReturnValue(value);
                                sybaseApiList.get(queue).setReadyToSendToTrue();
                                WorkingStats.okCountPlus();
                            }   break;    
       
                            
                        default:
                            value  = _SybaseCore.doAndReturnData(queue, true, true);
                            WorkingStats.testsCountPlus();
                            
                            if (_SybaseCore.isError()){
                                logger.error("Sybase core returned error.");
                                sybaseApiList.get(queue).addErrorReturnValue(_SybaseCore.getErrorDescription());
                                sybaseApiList.get(queue).setReadyToSendToTrue(); //done with errors
                                WorkingStats.errorCountPlus();
                            } else {
                                sybaseApiList.get(queue).addNormalReturnValue(value);
                                sybaseApiList.get(queue).setReadyToSendToTrue();
                                WorkingStats.okCountPlus();
                            }   break;
                    }

                    //dla wszystkich
                    sybaseApiList.get(queue).setIsNowExecutingToFalse();
                    stillExecuting=false;
                    //Runtime.getRuntime().gc();
                }


        }  catch (Exception e) {
            logger.error("--- Error in job " + queue + "! ----");
            sybaseApiList.get(queue).setIsNowRefreshingToFalse();
            sybaseApiList.get(queue).setIsNowExecutingToFalse();

            Thread.currentThread().interrupt();

            JobExecutionException e2 = new JobExecutionException(e);
            // this job will refire immediately
            e2.refireImmediately();
            throw e2;  
      } finally {
              if (!stillExecuting){
                  sybaseApiList.get(queue).setIsNowExecutingToFalse();
                  sybaseApiList.get(queue).setIsNowRefreshingToFalse();
                  if (!sybaseApiList.get(queue).isSendToZbx()) sybaseApiList.get(queue).setReadyToSendToFalse();
              } else {
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