/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.config;


import static biz.szydlowski.sybase4zabbix.WorkingObjects.CrontabGroupsList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.JdbcApiList;
import biz.szydlowski.sybase4zabbix.api.JdbcApi;
import biz.szydlowski.sybase4zabbix.api.SysmonApi;
import biz.szydlowski.utils.OSValidator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author szydlowskidom
 */
public class SysmonServer  {
  
   private List<SysmonApi> sysmonApiList = new ArrayList<>();
    
   private String _setting="setting/sysmon-server.xml";
    
    static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.config.SysmonServer");

       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
     public  SysmonServer (){
  
      
         if (OSValidator.isUnix()){
              String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
              _setting = absolutePath + "/" + _setting;
         }
            
       
          
         try {
                        
		File fXmlFile = new File(_setting);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
             
		logger.info("Read sysmon-scheduler " + _setting);
                
                
                NodeList  nList = doc.getElementsByTagName("sysmon");
                		 
		for (int temp = 0; temp < nList.getLength(); temp++) {
                
		   Node nNode = nList.item(temp);
		   if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                          Element eElement = (Element) nNode;
                          
                         SysmonApi sysmonApi = new SysmonApi ();
                       
                         sysmonApi.setName(getTagValue("name", eElement));
                                                                 
                         boolean isJdbcSource = false;

                         for (JdbcApi jdbc_sybase1 :  JdbcApiList) {
                               if (jdbc_sybase1.getInterfaceName().equalsIgnoreCase(getTagValue("sybase_jdbc", eElement))) {
                                   sysmonApi.setJdbcDriver(jdbc_sybase1);
                                   isJdbcSource = true;
                                   break;
                               }
                           }

                          if (!isJdbcSource){
                              //BUG20171118
                              sysmonApi.addErrorToConfigErrorList("Unknown JdbcSource " + getTagValue("sybase_jdbc", eElement));
                              logger.error("!isJdbcSource " + getTagValue("sybase_jdbc", eElement));
                          }
                            
                          sysmonApi.setSampleIntervalMs( getTagValue("sample_interval_ms", eElement));
                                  
                           String cron_expression = "";
                          
                           if (eElement.getElementsByTagName("cron_expression").item(0).hasAttributes()){
                            
                                NamedNodeMap  baseElmnt_attr = eElement.getElementsByTagName("cron_expression").item(0).getAttributes();
                                for (int i = 0; i <  baseElmnt_attr.getLength(); ++i) {
                                    Node attr =  baseElmnt_attr.item(i);
                                    logger.debug(attr.getNodeName() + " = " + attr.getNodeValue());
                                    if (attr.getNodeName().equals("group") && attr.getNodeValue().equalsIgnoreCase("true")){
                                        String group = getTagValue("cron_expression", eElement);
                                        if (CrontabGroupsList.containsKey(group)){
                                            cron_expression = CrontabGroupsList.get(group);
                                            logger.debug("Set crontab expression " + cron_expression + " -> " + group);
                                        } else {
                                           sysmonApi.addErrorToConfigErrorList("Crontab group not found " + group);
                                            logger.error("Crontab group not found " + group);
                                           cron_expression = "0 0 * * * ?";
                                        }
                                    }
                                   
                                }
                         }  else {
                             cron_expression = getTagValue("cron_expression", eElement);
                         }
                          
                          
                          String use_montable = getTagValue("use_montable", eElement);
                          
                          sysmonApi.setCronExpression(cron_expression);
                          sysmonApi.setUseMonTable(use_montable);
                          
                          sysmonApiList.add(sysmonApi);
                                   
		   }
		}
                
                logger.info("Read sysmon-server done");
                                
         }  catch (ParserConfigurationException | SAXException | IOException e) {         
                logger.fatal("sysmon-server.xml::XML Exception/Error:", e);
                System.exit(-1);
				
	  }
    }
    

  
   private static String getTagValue(String sTag, Element eElement) {
	
	try {
            NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
            Node nValue = (Node) nlList.item(0);
            return nValue.getNodeValue();
        } catch (Exception e){
            if (sTag.equals("use_montable")) {
                  return "false";
            } else {
                 logger.error("getTagValue error " + sTag + " "+ e);
                 return "ERROR";
            }
           
        }

  }

  public List<SysmonApi> getSysmonApiList (){
          return  sysmonApiList;
  }
   

}
