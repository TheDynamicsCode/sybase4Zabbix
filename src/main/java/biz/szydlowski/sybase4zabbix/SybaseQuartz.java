/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;


import static biz.szydlowski.sybase4zabbix.Sybase4ZabbixDaemon.scheduler;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.sybaseApiList;
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
 * @author szydlowskidom
 */
public class SybaseQuartz {
    
     static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.SybaseQuartz");
     
    private boolean isDo = false;
 
    
    public SybaseQuartz(){
    }
     
    public void doJob( ){
      
        if (isDo){
            logger.fatal("SybaseQuartz is running");
            return;
        } else {
            isDo = true;
        }
                  
    
        try {
                int queue = 0;
                int queue_active = 0;
                
                List<JobDetail> jobs = new ArrayList<>();
                List<Trigger> triggers = new ArrayList<>();
                logger.debug("Jobs " + sybaseApiList.size());                    
               
                for (queue=0; queue<sybaseApiList.size(); queue++){
                    
                  sqlType sql_type = sybaseApiList.get(queue).getSqlType();
             
                  switch (sql_type) {
                        case SYSMON:
                        case SYSMON_WITH_DISCOVERY:
                        case SYSMON_WITH_DISCOVERY_EMBEDDED:
                            //logger.debug("Job " + sybaseApiList.get(queue).getAlias() + " is SYSMON QUERY ...");
                            continue;
                            //2 typy nie obługiwane tutaj
                        case DISCOVERY_STATIC:
                        case DISCOVERY_QUERY:
                        case DISCOVERY_EMBEDDED:
                            logger.debug("Job " + sybaseApiList.get(queue).getAlias() + " is DISCOVERY_QUERY...");
                            logger.debug("Add job " + sybaseApiList.get(queue).getAlias() + "." + queue);
                            sybaseApiList.get(queue).setReadyToSendToFalse();
                            jobs.add(JobBuilder.newJob(SybaseQuartzJob.class).withIdentity(sybaseApiList.get(queue).getAlias() + "." + queue, "Zabbix.Discovery.Sybase").build());
                            jobs.get(queue_active).getJobDataMap().put("queue", queue);
                            queue_active++;
                            triggers.add(TriggerBuilder.newTrigger()
                                    .withIdentity("trigger." + sybaseApiList.get(queue).getAlias()  + queue, "SYBASE")
                                    .withSchedule(CronScheduleBuilder.cronSchedule(sybaseApiList.get(queue).getCronExpression()))
                                    .build());
                            jobs.add(JobBuilder.newJob(SybaseQuartzJob.class).withIdentity(sybaseApiList.get(queue).getAlias() + ".now." + queue, "Zabbix.Discovery.Sybase").build());
                            jobs.get(queue_active).getJobDataMap().put("queue", queue);
                            queue_active++;
                            Trigger runOnceTrigger = TriggerBuilder.newTrigger().withIdentity("trigger.now." + sybaseApiList.get(queue).getAlias()  + queue).build();
                            triggers.add(runOnceTrigger);
                            break;
                        default:
                            logger.debug("Add job " + sybaseApiList.get(queue).getAlias() + "." + queue);
                            sybaseApiList.get(queue).setReadyToSendToFalse();
                            sybaseApiList.get(queue).setIsNowExecutingToFalse();
                            jobs.add(JobBuilder.newJob(SybaseQuartzJob.class).withIdentity(sybaseApiList.get(queue).getAlias() + "." + queue, "Zabbix.Sybase").build());
                            jobs.get(queue_active).getJobDataMap().put("queue", queue);
                            queue_active++;
                            triggers.add(TriggerBuilder.newTrigger()
                                    .withIdentity("trigger." + sybaseApiList.get(queue).getAlias()  + queue, "SYBASE")
                                    .withSchedule(CronScheduleBuilder.cronSchedule(sybaseApiList.get(queue).getCronExpression()))
                                    .build());
                            
                            if (sybaseApiList.get(queue).isFireAtStartup()){
                                jobs.add(JobBuilder.newJob(SybaseQuartzJob.class).withIdentity( sybaseApiList.get(queue).getAlias() + ".runAtStartup." + queue, "Zabbix.Sybase").build());
                                jobs.get(queue_active).getJobDataMap().put("queue", queue);

                                queue_active++;
                                Trigger runAtStartupTrigger = TriggerBuilder.newTrigger().withIdentity("runAtStartup.now." +  sybaseApiList.get(queue).getAlias()  + queue).build();
                                triggers.add(runAtStartupTrigger);
                            }
                            break;
                    }
                
            
                } 
          
                 //schedule it
                 if (!scheduler.isStarted()){
                       logger.info("Start scheduler");
                       scheduler.start();
                 }  
                
                
                for (int ii=0; ii<jobs.size(); ii++){
                    scheduler.scheduleJob(jobs.get(ii), triggers.get(ii));
                }
    
            
        }
        catch(SchedulerException e){
           logger.error(e);
        }
        
    
    }
   
    public boolean stop(){

       isDo = false;
       
       try {
            
            if (scheduler.isStarted()){
                scheduler.clear();
                scheduler.shutdown();
            }           
            return true;
       }  catch(SchedulerException e){
            logger.error(e);
            return false;
       }
    }
    
    public String refreshTask(int _queue_task){
      
        if (isDo){
            
            if (!sybaseApiList.get(_queue_task).isActiveMode()){
               return "The task " + sybaseApiList.get(_queue_task).getAlias()  + " is in inactive mode, you cannot refresh it.";
            }
       
            if  ( ! sybaseApiList.get(_queue_task).isNowRefreshing() ){
                
                if  ( sybaseApiList.get(_queue_task).isNowExecuting() ){
                   return "The task " + sybaseApiList.get(_queue_task).getAlias()  + " is executing now, you cannot refresh it.";
                } 
                
                
                // if you don't call startAt() then the current time (immediately) is assumed.
                Trigger runOnceTrigger = TriggerBuilder.newTrigger().withIdentity("SYBASE.trigger.now."+ _queue_task, "SYBASE").build();
                JobDetail job = JobBuilder.newJob(SybaseQuartzJob.class)
                        .withIdentity("refresh."+sybaseApiList.get(_queue_task).getAlias()+"."+_queue_task, "SYBASE").build();
               
                job.getJobDataMap().put("queue", _queue_task);
                sybaseApiList.get(_queue_task).setIsNowRefreshing();
               
                try {
                    scheduler.scheduleJob(job, runOnceTrigger);
                    return "The task " + sybaseApiList.get(_queue_task).getAlias() + " was scheduled.";
                } catch (SchedulerException e){
                    logger.error(e);
                    return e.getMessage();
                }             

            } else {
                return  "The task " + sybaseApiList.get(_queue_task).getAlias()  + " is refreshing now, you cannot refresh it again.";  
               
            }
        } else {
            return "The task is not monitoring.";
        }
   
    } 
}
