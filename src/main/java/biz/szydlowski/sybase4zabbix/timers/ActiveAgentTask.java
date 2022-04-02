/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.timers;


import static biz.szydlowski.sybase4zabbix.Sybase4ZabbixDaemon.zabbixServerApiList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.sybaseApiList;
import biz.szydlowski.sybase4zabbix.sqlType;
import biz.szydlowski.utils.WorkingStats;
import static biz.szydlowski.utils.tasks.TasksWorkspace.ActiveAgentTimer_lock;
import biz.szydlowski.zabbixmon.DataObject;
import biz.szydlowski.zabbixmon.SenderResult;
import biz.szydlowski.zabbixmon.ZabbixSender;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;
import static biz.szydlowski.utils.tasks.TasksWorkspace.ActiveAgentTimer_time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author dkbu
 */
public class ActiveAgentTask extends TimerTask  {
            static final Logger logger =  LogManager.getLogger(ActiveAgentTask.class);
            //new
            List<DataObject> dataObject_s = new ArrayList<>();
            List<DataObject> dataObject_nsender = new ArrayList<>();
            int maxSenderPacketSize = 200;
            int qSender=0;
            
            StringBuilder k = new StringBuilder();
            SenderResult result; 
            String zbkey = "zbkey";
        
            List<ZabbixSender> zabbixSenderList = new ArrayList<>();
            boolean firstRun = true;
              
            @Override
            public void run() {
                //#BUG 20170608
                ActiveAgentTimer_time = System.currentTimeMillis();
                
                if (firstRun){
                    logger.info("Setting zabbixServerApi in ActiveAgentTask....");
                    firstRun=false;
                    for (ZabbixServerApi zabbixServerApi : zabbixServerApiList){
                      
                           ZabbixSender zabbixSender = new ZabbixSender(zabbixServerApi.getHost(), zabbixServerApi.getPort(), zabbixServerApi.getConnectTimeout(), zabbixServerApi.getSocketTimeout());
                           zabbixSenderList.add(zabbixSender);
                          
                    }
                }
                
                if (ActiveAgentTimer_lock){
                    logger.info("ActiveAgentTimer is locked");
                    return;
                } else {
                    ActiveAgentTimer_lock=true;
                }
                           
                try {
                    for (int listServer=0; listServer<zabbixServerApiList.size(); listServer++){

                           boolean setAgent=false;
                          
                           if (dataObject_s!=null) dataObject_s.clear(); else
                           { 
                               dataObject_s = new ArrayList<>();
                           }
                           

                           for (int queue=0; queue<sybaseApiList.size(); queue++){

                              if (!sybaseApiList.get(queue).getZabbixServerName().equalsIgnoreCase(zabbixServerApiList.get(listServer).getServerName())){
                                  continue;
                              } else {
                                  setAgent = true;
                              }   


                              boolean done = sybaseApiList.get(queue).isReadyToSend();
                              boolean embedded_discovery = false;
                              boolean multiple = false;
                              boolean sysmon_discovery = false;
                              boolean discovery = false;

                              if (sybaseApiList.get(queue).getSqlType()==sqlType.EMBEDDED_WITH_DISCOVERY || 
                                      sybaseApiList.get(queue).getSqlType()==sqlType.EMBEDDED_WITH_DISCOVERY_EMBEDDED){
                                  embedded_discovery=true;
                              } 

                              if (sybaseApiList.get(queue).getSqlType()==sqlType.PURE_WITH_MULTIPLE ||
                                      sybaseApiList.get(queue).getSqlType()==sqlType.PREDEFINED_WITH_MULTIPLE){
                                  multiple=true;
                              }

                              if (sybaseApiList.get(queue).getSqlType()==sqlType.SYSMON_WITH_DISCOVERY ||
                                     sybaseApiList.get(queue).getSqlType()==sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED){
                                  sysmon_discovery=true;
                              } 

                              if (sybaseApiList.get(queue).getSqlType()==sqlType.DISCOVERY_QUERY ||
                                      sybaseApiList.get(queue).getSqlType()==sqlType.DISCOVERY_EMBEDDED || 
                                      sybaseApiList.get(queue).getSqlType()==sqlType.DISCOVERY_STATIC ){
                                   discovery=true;
                              }

                              if (done){
                                  if (logger.isDebugEnabled()) logger.debug("Prepare to sending throw active agent using " + sybaseApiList.get(queue).getZabbixKey());
                                
                                  try {
                                  
                                      if (discovery){
                                            //logger.debug("**** DISCOVERY ****");
                                          
                                           // List<List<String>> getData  = sybaseApiList.get(queue).getDiscoveryMap();

                                            if (sybaseApiList.get(queue).getDiscoveryMap()!=null){
                                               if (sybaseApiList.get(queue).getDiscoveryMap().size()>0){
                                            
                                                // List<String> metaData = sybaseApiList.get(queue).getDiscoveryMetaData();
                                                  
                                                 DataObject dataObject = new DataObject();

                                                  dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                  dataObject.setKey(sybaseApiList.get(queue).getZabbixKey());
                                                 
                                                  JSONObject data = new JSONObject();
                                                  List<JSONObject> aray = new LinkedList<>();  

                                                  for(int i=0; i<sybaseApiList.get(queue).getDiscoveryMap().size();i++) {
                                                      
                                                     //   List <String> keys_value  = getData.get(i);
                                                        if (sybaseApiList.get(queue).getDiscoveryMap().get(i).size()!=sybaseApiList.get(queue).getDiscoveryMetaData().size()){
                                                            logger.error("Consist error metaData");
                                                            continue;
                                                        }
                                                        //#BUG 20170818 
                                                        if (sybaseApiList.get(queue).getDiscoveryMetaData().isEmpty() || sybaseApiList.get(queue).getDiscoveryMap().get(i).isEmpty()) continue;

                                                        JSONObject xxx = new JSONObject(); 

                                                        for (int j=0; j<sybaseApiList.get(queue).getDiscoveryMetaData().size();j++){                                                   

                                                          // k.setLength(0);
                                                           k.delete(0, k.length());
                                                           k.append("{#").append(sybaseApiList.get(queue).getDiscoveryMetaData().get(j)).append("}"); 
                                                           xxx.put(k.toString(), sybaseApiList.get(queue).getDiscoveryMap().get(i).get(j));

                                                        }  

                                                        aray.add(xxx);

                                                   }  

                                                  data.put("data", aray);
                                                  //System.out.println(data.toJSONString());
                                                  logger.debug(data.toJSONString());

                                                  dataObject.setValue(data.toJSONString());
                                                  dataObject.setClock(System.currentTimeMillis()/1000);
                                                  
                                                  //+++
                                                  data = null;
                                                  aray = null;

                                                  dataObject_s.add(dataObject); 

                                               }

                                            }

                                      } else  if ( embedded_discovery || sysmon_discovery){
                                          
                                          //  List<List<String>> getData = sybaseApiList.get(queue).getDiscoveryMap();
                                            if (sybaseApiList.get(queue).getDiscoveryMap()!=null){
                                                if (sybaseApiList.get(queue).getDiscoveryMap().size()>0){
                                                  //  List<String> metaData = sybaseApiList.get(queue).getDiscoveryMetaData();

                                                    for(int i=0; i<sybaseApiList.get(queue).getDiscoveryMap().size(); i++) {
                                                        
                                                       // List <String> keys_value  = getData.get(i);
                                                        if (sybaseApiList.get(queue).getDiscoveryMap().get(i).size()!=sybaseApiList.get(queue).getDiscoveryMetaData().size()){
                                                                logger.error("Consist error metaData");
                                                                continue;
                                                        }

                                                       //#BUG 20170818 
                                                       if (sybaseApiList.get(queue).getDiscoveryMetaData().isEmpty() || sybaseApiList.get(queue).getDiscoveryMap().get(i).isEmpty()) continue;

                                                       zbkey = sybaseApiList.get(queue).getZabbixKey();
                                                       for (int j=0; j<sybaseApiList.get(queue).getDiscoveryMap().get(i).size();j++){
                                                            String mtd = "{#"+sybaseApiList.get(queue).getDiscoveryMetaData().get(j)+"}";
                                                            zbkey=zbkey.replace(mtd.toUpperCase(), sybaseApiList.get(queue).getDiscoveryMap().get(i).get(j));
                                                       }
                                                       //#BUG 20170818 
                                                       if (i>=sybaseApiList.get(queue).getReturnListValue().size()){
                                                           logger.error("Consist error returnListValue");
                                                           continue;
                                                       }  

                                                       if (i>=sybaseApiList.get(queue).getStateList().size()){
                                                           logger.error("Consist error getStateList");
                                                           continue;
                                                       }

                                                       logger.debug(zbkey + ":" + sybaseApiList.get(queue).getReturnListValue().get(i));

                                                      
                                                       if (sybaseApiList.get(queue).getSqlQuery().equals("sybase_db_space")){
                                                            boolean allInOne =  Boolean.parseBoolean(sybaseApiList.get(queue).getDatabaseProperty("allInOne", "false"));
                                                            if (allInOne){
                                                               
                                                                String values[] = sybaseApiList.get(queue).getReturnListValue().get(i).split(";");
                                                                if (values.length!=4){
                                                                      logger.error("allInOne values.length!=4");
                                                                } else {
                                                                      //free, free-prc, used, used-prc
                                                                     String zbkeytmp = zbkey.replace("{#OPTION}", "free");
                                                                     DataObject dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[0]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject); 
                                                                    
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "free_prc");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[1]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                                  
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "used");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[2]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                              
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "used_prc");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[3]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                                
                                                              }
                                                                

                                                            } else {
                                                               DataObject dataObject = new DataObject();
                                                                dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                dataObject.setKey(zbkey);
                                                                dataObject.setValue(sybaseApiList.get(queue).getReturnListValue().get(i));
                                                                dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                dataObject.setClock(System.currentTimeMillis()/1000);

                                                                dataObject_s.add(dataObject); 
                                                            }
                                                    
                                                       } else if (sybaseApiList.get(queue).getSqlQuery().equals("sybase_table_space")){
                                                                String values[] = sybaseApiList.get(queue).getReturnListValue().get(i).split(";");
                                                                if (values.length!=6){
                                                                      logger.error("allInOne values.length!=6");
                                                                } else {
                                                                    /*
                                                                       space[0]=rowtotal;
                                                                        space[1]=reserved/1024.0;
                                                                        space[2]=data/1024.0;
                                                                        space[3]=index_size/1024.0;
                                                                        space[4]=text_size/1024.0;
                                                                        space[5]=unused/1024.0;
                                                                    */
                                                                    
                                                                     String zbkeytmp = zbkey.replace("{#OPTION}", "rowtotal");
                                                                     DataObject dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[0]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject); 
                                                                    
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "reserved");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[1]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                                  
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "data");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[2]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                              
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "index_size");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[3]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                                     
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "text_size");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[4]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                                     
                                                                     zbkeytmp = zbkey.replace("{#OPTION}", "unused");
                                                                     
                                                                     dataObject = new DataObject();
                                                                     dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                                     dataObject.setKey(zbkeytmp);
                                                                     dataObject.setValue(values[5]);
                                                                     dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                                     dataObject.setClock(System.currentTimeMillis()/1000);
                                                                     dataObject_s.add(dataObject);
                                                                
                                                              }

                                                    
                                                       } else { 
                                                            DataObject dataObject = new DataObject();
                                                            dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                            dataObject.setKey(zbkey);
                                                            dataObject.setValue(sybaseApiList.get(queue).getReturnListValue().get(i));
                                                            dataObject.setState(sybaseApiList.get(queue).getStateList().get(i));
                                                            dataObject.setClock(System.currentTimeMillis()/1000);

                                                            dataObject_s.add(dataObject);
                                                       }
                                                       
                                                     

                                                    }
                                                }

                                            }
                                     } else  if (multiple){
                                          
                                          //  List<List<String>> getData = sybaseApiList.get(queue).getDiscoveryMap();
                                            if (sybaseApiList.get(queue).getDiscoveryMap()!=null){
                                                if (sybaseApiList.get(queue).getDiscoveryMap().size()>0){
                                                  
                                                   // List<String> metaData = sybaseApiList.get(queue).getDiscoveryMetaData();

                                                    int valcol=-1;

                                                    for (int j=0; j< sybaseApiList.get(queue).getDiscoveryMetaData().size();j++){
                                                       if ( sybaseApiList.get(queue).getDiscoveryMetaData().get(j).equalsIgnoreCase("VALUE")){
                                                           valcol=j;
                                                           break;
                                                       }
                                                    }

                                                    if (valcol>-1){

                                                        for(int i=0; i<sybaseApiList.get(queue).getDiscoveryMap().size(); i++) {
                                                          
                                                          //  List <String> keys_value  = getData.get(i);
                                                            if (sybaseApiList.get(queue).getDiscoveryMap().get(i).size()!= sybaseApiList.get(queue).getDiscoveryMetaData().size()){
                                                                    logger.error("Consist error metaData");
                                                                    continue;
                                                            }  //#BUG 20170818 
                                                            if ( sybaseApiList.get(queue).getDiscoveryMetaData().isEmpty() || sybaseApiList.get(queue).getDiscoveryMap().get(i).isEmpty()) continue;

                                                           zbkey = sybaseApiList.get(queue).getZabbixKey();
                                                           for (int j=0; j<sybaseApiList.get(queue).getDiscoveryMap().get(i).size();j++){
                                                                if (j==valcol) continue;
                                                                String mtd = "{#"+ sybaseApiList.get(queue).getDiscoveryMetaData().get(j)+"}";
                                                                zbkey=zbkey.replace(mtd.toUpperCase(), sybaseApiList.get(queue).getDiscoveryMap().get(i).get(j));
                                                           }

                                                            DataObject dataObject = new DataObject();
                                                            dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                            dataObject.setKey(zbkey);
                                                            dataObject.setValue(sybaseApiList.get(queue).getDiscoveryMap().get(i).get(valcol));
                                                            dataObject.setState(0);
                                                            dataObject.setClock(System.currentTimeMillis()/1000);

                                                            dataObject_s.add(dataObject); 
                                                            
                                                            logger.debug(dataObject.toString());

                                                        }
                                                    } else {
                                                        logger.error("Value data not found!!! in " + sybaseApiList.get(queue).getAlias());
                                                    }
                                                }
                                            } else {
                                                  logger.error("getData is null " + sybaseApiList.get(queue).getAlias());  
                                            }
                                     }  else {
                                           if (sybaseApiList.get(queue).getSqlQuery().equals("sybase_db_space")){
                                                boolean allInOne =  Boolean.parseBoolean(sybaseApiList.get(queue).getDatabaseProperty("allInOne", "false"));
                                                zbkey = sybaseApiList.get(queue).getZabbixKey();
                                                if (allInOne){

                                                    String values[] = sybaseApiList.get(queue).getReturnListValue().get(0).split(";");
                                                    if (values.length!=4){
                                                          logger.error("allInOne values.length!=4");
                                                    } else {
                                                          //free, free-prc, used, used-prc
                                                         String zbkeytmp = zbkey.replace("{#OPTION}", "free");
                                                         DataObject dataObject = new DataObject();
                                                         dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                         dataObject.setKey(zbkeytmp);
                                                         dataObject.setValue(values[0]);
                                                         dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                         dataObject.setClock(System.currentTimeMillis()/1000);
                                                         dataObject_s.add(dataObject); 

                                                         zbkeytmp = zbkey.replace("{#OPTION}", "free_prc");

                                                        dataObject = new DataObject();
                                                         dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                         dataObject.setKey(zbkeytmp);
                                                         dataObject.setValue(values[1]);
                                                         dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                         dataObject.setClock(System.currentTimeMillis()/1000);
                                                         dataObject_s.add(dataObject);
                                                    
                                                         zbkeytmp = zbkey.replace("{#OPTION}", "used");

                                                         dataObject = new DataObject();
                                                         dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                         dataObject.setKey(zbkeytmp);
                                                         dataObject.setValue(values[2]);
                                                         dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                         dataObject.setClock(System.currentTimeMillis()/1000);
                                                         dataObject_s.add(dataObject);

                                                         zbkeytmp = zbkey.replace("{#OPTION}", "used_prc");

                                                         dataObject = new DataObject();
                                                         dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                         dataObject.setKey(zbkeytmp);
                                                         dataObject.setValue(values[3]);
                                                         dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                         dataObject.setClock(System.currentTimeMillis()/1000);
                                                         dataObject_s.add(dataObject);
                                                  }


                                                }else {
                                                    DataObject dataObject = new DataObject();
                                                    dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                    dataObject.setKey(zbkey);
                                                    dataObject.setValue(sybaseApiList.get(queue).getReturnListValue().get(0));
                                                    dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                    dataObject.setClock(System.currentTimeMillis()/1000);

                                                    dataObject_s.add(dataObject); 
                                                }

                                           } else if (sybaseApiList.get(queue).getSqlQuery().equals("sybase_table_space")){
                                                        String values[] = sybaseApiList.get(queue).getReturnListValue().get(0).split(";");
                                                        if (values.length!=6){
                                                              logger.error("allInOne values.length!=6");
                                                        } else {
                                                            /*
                                                               space[0]=rowtotal;
                                                                space[1]=reserved/1024.0;
                                                                space[2]=data/1024.0;
                                                                space[3]=index_size/1024.0;
                                                                space[4]=text_size/1024.0;
                                                                space[5]=unused/1024.0;
                                                            */

                                                             String zbkeytmp = zbkey.replace("{#OPTION}", "rowtotal");
                                                             DataObject dataObject = new DataObject();
                                                             dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                             dataObject.setKey(zbkeytmp);
                                                             dataObject.setValue(values[0]);
                                                             dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                             dataObject.setClock(System.currentTimeMillis()/1000);
                                                             dataObject_s.add(dataObject); 

                                                             zbkeytmp = zbkey.replace("{#OPTION}", "reserved");

                                                             dataObject = new DataObject();
                                                             dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                             dataObject.setKey(zbkeytmp);
                                                             dataObject.setValue(values[1]);
                                                             dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                             dataObject.setClock(System.currentTimeMillis()/1000);
                                                             dataObject_s.add(dataObject);

                                                             zbkeytmp = zbkey.replace("{#OPTION}", "data");

                                                             dataObject = new DataObject();
                                                             dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                             dataObject.setKey(zbkeytmp);
                                                             dataObject.setValue(values[2]);
                                                             dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                             dataObject.setClock(System.currentTimeMillis()/1000);
                                                             dataObject_s.add(dataObject);

                                                             zbkeytmp = zbkey.replace("{#OPTION}", "index_size");

                                                             dataObject = new DataObject();
                                                             dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                             dataObject.setKey(zbkeytmp);
                                                             dataObject.setValue(values[3]);
                                                             dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                             dataObject.setClock(System.currentTimeMillis()/1000);
                                                             dataObject_s.add(dataObject);

                                                             zbkeytmp = zbkey.replace("{#OPTION}", "text_size");

                                                             dataObject = new DataObject();
                                                             dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                             dataObject.setKey(zbkeytmp);
                                                             dataObject.setValue(values[4]);
                                                             dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                             dataObject.setClock(System.currentTimeMillis()/1000);
                                                             dataObject_s.add(dataObject);

                                                             zbkeytmp = zbkey.replace("{#OPTION}", "unused");

                                                             dataObject = new DataObject();
                                                             dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                             dataObject.setKey(zbkeytmp);
                                                             dataObject.setValue(values[5]);
                                                             dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                             dataObject.setClock(System.currentTimeMillis()/1000);
                                                             dataObject_s.add(dataObject);

                                                      }

                                                    
                                             } else { 
                                                 DataObject dataObject = new DataObject();
                                                  dataObject.setHost(sybaseApiList.get(queue).getZabbixHost());
                                                  dataObject.setKey(sybaseApiList.get(queue).getZabbixKey());
                                                  dataObject.setValue(sybaseApiList.get(queue).getReturnListValue().get(0));
                                                  dataObject.setState(sybaseApiList.get(queue).getStateList().get(0));
                                                  dataObject.setClock(System.currentTimeMillis()/1000);

                                                  dataObject_s.add(dataObject);
                                           }
                                         
                                    }

                                     sybaseApiList.get(queue).setReadyToSendToFalse();
                                     
                                } catch (Exception e){
                                    logger.error("KERNEL PANIC " + e.getMessage() + " for " + sybaseApiList.get(queue).getZabbixKey()); 
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                    WorkingStats.kernelPanicPlus();
                                } finally {
                                    sybaseApiList.get(queue).setReadyToSendToFalse();
                                }

                             } //end done

                        }

                        if (dataObject_s.size()>0) {
                                                      
                            if (setAgent){
                                
                                maxSenderPacketSize = zabbixServerApiList.get(listServer).getMaxSenderPacketSize();
                                if (maxSenderPacketSize==-1) maxSenderPacketSize=dataObject_s.size(); //unlimited
                                
                                logger.debug("dataObject to send size - " + dataObject_s.size());
                                 
                                qSender=0;
                                if (dataObject_nsender==null) dataObject_nsender = new ArrayList<>();
                                
                                while (qSender<dataObject_s.size()){
                                   
                                    for (int w=1; w<=maxSenderPacketSize; w++){
                                       // logger.trace("add object to send nb - " + qSender);
                                        dataObject_nsender.add(dataObject_s.get(qSender));
                                        qSender++;
                                        if (qSender==dataObject_s.size()) break;
                                    }
                                  
                                    logger.trace("current parts to send size - " + dataObject_nsender.size());
                                 
                                    if (dataObject_nsender.size()>0) {
                                       
                                           logger.debug("dataObject_nsender:" + dataObject_nsender.toString());
                                            result = zabbixSenderList.get(listServer).send(dataObject_nsender);
                                      
                                            WorkingStats.zabbixProcessedCountPlus(result.getProcessed());
                                            //#BUG20170903
                                            WorkingStats.zabbixFailedCountPlus(result.getFailed());
                                            WorkingStats.zabbixTotalCountPlus(result.getTotal());
                                        
                                        
                                            if (result.isConnError()){
                                                WorkingStats.connErrorPlus();
                                            }
                                     
                                    
                                    }
                                    
                                    dataObject_nsender.clear();
                                }

                            } else {
                                logger.error("Problem with set active agent???? " +zabbixServerApiList.get(listServer).getServerName());
                            }

                        }

                        dataObject_s.clear();


                    }
                } catch (Exception e){
                    logger.error("AGENT ERROR " + e.getMessage()); 
                    ActiveAgentTimer_time = System.currentTimeMillis();
                    ActiveAgentTimer_lock=false;
                    WorkingStats.kernelPanicPlus();
                } finally {
                    // dataObject_s = null;
                    // dataObject_nsender = null;
                     ActiveAgentTimer_time = System.currentTimeMillis();
                     ActiveAgentTimer_lock=false;
                }
             
           };   
    } 
