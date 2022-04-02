/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;

import static biz.szydlowski.sybase4zabbix.Sybase4ZabbixDaemon.SysmonEngineList;
import static biz.szydlowski.sybase4zabbix.Sybase4ZabbixDaemon.scheduler_sysmon;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.sybaseApiList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.sysmonApiList;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 *
 * @author dominik
 */
public class SysmonQuartz  {
    
     static final Logger logger =  LogManager.getLogger(SysmonQuartz.class);
  
    private boolean isDo = false;

     
    public SysmonQuartz(){

               
    }
     
    public void doJob(){
        
        if (isDo){
            logger.fatal("SysmonQuartz is running");
            return;
        } else {
            logger.info("*** SysmonQuartz starting ****");
            logger.info("*** INIT  SysmonEngineList ****");
            SysmonEngineList = new ArrayList<>();
            for (int i=0;i<sysmonApiList.size();i++){
                SysmonEngineList.add( new SysmonEngine(sysmonApiList.get(i).getSampleIntervalMs()));
            }
            if (SysmonEngineList==null) logger.fatal("SysmonEngineList==null");
            isDo = true;
        }
                  
             
    
        try {
                int queue = 0;
                
                List<JobDetail> jobs = new ArrayList<>();
                List<Trigger> triggers = new ArrayList<>();
                logger.debug("Jobs " +  sysmonApiList.size());
                
                for (int i=0; i< sybaseApiList.size(); i++){ 
                    sqlType type = sybaseApiList.get(i).getSqlType();
                    
                    if (type == sqlType.SYSMON || type == sqlType.SYSMON_WITH_DISCOVERY ||  type == sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED ){ 
                        String _name = sybaseApiList.get(i).getSysmonName();
                        for (int j=0; j<sysmonApiList.size(); j++){
                            if (_name.equalsIgnoreCase( sysmonApiList.get(j).getName() ) ){
                                sybaseApiList.get(j).setReadyToSendToFalse();
                            }
                        }
                        
                    }
                   
                }
                              
               
                for (queue=0; queue<sysmonApiList.size(); queue++){
                                  
                    logger.debug("Add job " + sysmonApiList.get(queue).getName() + "." + queue);
                                 
                    jobs.add(JobBuilder.newJob(SysmonQuartzJob.class).withIdentity( sysmonApiList.get(queue).getName() + "." + queue, "Zabbix.Sysmon").build());
                                   
                    jobs.get(queue).getJobDataMap().put("sysmon_api_queue", queue);
              
                    triggers.add(TriggerBuilder
                        .newTrigger()
                        .withIdentity("trigger." +  sysmonApiList.get(queue).getName()  + queue, "SYSMON")
                        .withSchedule(CronScheduleBuilder.cronSchedule( sysmonApiList.get(queue).getCronExpression() ))
                        .build());
                
            
                } 
          
                 //schedule it
                 if (!scheduler_sysmon.isStarted()){
                       logger.info("Start scheduler_sysmon");
                       scheduler_sysmon.start();
                 }  
                
                
                for (int ii=0; ii<jobs.size(); ii++){
                    scheduler_sysmon.scheduleJob(jobs.get(ii), triggers.get(ii));
                }
    
            
        }
        catch(SchedulerException e){
           logger.error(e);
        }
        
    
    }
   
    public boolean stop(){
       isDo = false;
       
       try {
            
            if (scheduler_sysmon.isStarted()){
                scheduler_sysmon.clear();
                scheduler_sysmon.shutdown();
            }       
       }  catch(SchedulerException e){
            logger.error(e);
       } finally {
          logger.info("*** SysmonQuartz stopped ****");
        
          for (int i=0;i< SysmonEngineList.size();i++){
                SysmonEngineList.get(i).destroySysmon();
          }
          
          SysmonEngineList.clear();
          SysmonEngineList = null;
          
          logger.info("*** SysmonEngineList was cleared ****");
       }
       
       return true;
      
    }
    
    
    public String refreshTask(int _queue_task){
        
       
        if (isDo){
            
            if (!sysmonApiList.get(_queue_task).isActiveMode()){
               return "The SYSMON task " + sysmonApiList.get(_queue_task).getName()  + " is in inactive mode, you cannot refresh it.";
            }
       
            if  ( ! sysmonApiList.get(_queue_task).isNowRefreshing() ){
                
                if  ( sysmonApiList.get(_queue_task).isNowExecuting() ){
                   return "The SYSMON task " + sysmonApiList.get(_queue_task).getName()  + " is executing now, you cannot refresh it.";
                } 
                
                
                // if you don't call startAt() then the current time (immediately) is assumed.
                Trigger runOnceTrigger = TriggerBuilder.newTrigger().withIdentity("SYBASE.trigger.now."+ _queue_task, "SYBASE").build();
                JobDetail job = JobBuilder.newJob(SysmonQuartzJob.class)
                        .withIdentity("refresh."+sysmonApiList.get(_queue_task).getName()+"."+_queue_task, "SYBASE").build();
               
               //bug 20171111 
                job.getJobDataMap().put("sysmon_api_queue", _queue_task);
                sysmonApiList.get(_queue_task).setIsNowRefreshing();
               
                try {
                    scheduler_sysmon.scheduleJob(job, runOnceTrigger);
                    return "The SYSMON task " + sysmonApiList.get(_queue_task).getName() + " was scheduled.";
                } catch (SchedulerException e){
                    logger.error(e);
                    return e.getMessage();
                }             

            } else {
                return  "The SYSMON task " + sysmonApiList.get(_queue_task).getName()  + " is refreshing now, you cannot refresh it again.";  
               
            }
        } else {
            return "The SYSMON task is not monitoring.";
        }
   
    } 
}