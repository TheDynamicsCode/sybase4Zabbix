/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.config;


import static biz.szydlowski.sybase4zabbix.Sybase4ZabbixDaemon.zabbixServerApiList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.CrontabGroupsList;
import static biz.szydlowski.sybase4zabbix.WorkingObjects.JdbcApiList;
import biz.szydlowski.sybase4zabbix.api.JdbcApi;
import biz.szydlowski.sybase4zabbix.api.QueryApi;
import biz.szydlowski.sybase4zabbix.api.SybaseApi;
import biz.szydlowski.sybase4zabbix.api.SysmonApi;
import biz.szydlowski.sybase4zabbix.sqlType;
import biz.szydlowski.utils.OSValidator;
import biz.szydlowski.utils.WorkingStats;
import biz.szydlowski.zabbixmon.ZabbixServerApi;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author dominik
 */
public class SybaseParams {
   
   private List<SysmonApi> sysmonApiList = new ArrayList<>();
   private final List<SybaseApi> sybaseApiList = new ArrayList<>();
   private List<QueryApi> queryList =  new ArrayList<>(); 
   
    static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.config.SybaseParams");
    
       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
     public SybaseParams(){
           
        
       
        sysmonApiList = new SysmonServer().getSysmonApiList();
                
        SqlQuery _SqlQuery = new SqlQuery();
        queryList =  _SqlQuery.getQueryApiList();
       
        Properties filenames = new SybaseParamsFiles().getProperties();
        Set<Object> keys = getAllKeys(filenames);
        for(Object k:keys){
            String key = (String)k; 
            String set_filename = getPropertyValue(filenames, key);
            if (OSValidator.isUnix()){
              String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                   absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
               set_filename = absolutePath + "/setting/" + set_filename;
            } else {
                set_filename =  "setting/" + set_filename;
            }
          
            addPropsFromFile(set_filename);
            //logger.debug("props size = "  + props.size());
        }
        
        logger.info("Total sybaseApi size = "  + sybaseApiList.size());
 
       
    }
     
    
    private void addPropsFromFile(String filename){
        try {
                        
		File fXmlFile = new File(filename);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
               
                String prefix1 = doc.getElementsByTagName("params").item(0).getAttributes().getNamedItem("prefix").getNodeValue();
             
		logger.info("Read sybase-to-zabbix " + filename);
                           
		NodeList  nList = doc.getElementsByTagName("sybaseToZabbix");
                Node nNode;
                		 
		for (int temp = 0; temp < nList.getLength(); temp++) {
                
		   nNode = nList.item(temp);
		   if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                          
                          //String agent = nNode.getAttributes().getNamedItem("agent").getNodeValue();
                          String itemhost =  "";// nNode.getAttributes().getNamedItem("host").getNodeValue();
                          String prefix2 =  "";//nNode.getAttributes().getNamedItem("prefix").getNodeValue(); 
                          String active="true";
                          
                          String  zabbix_server_host="default";
                          String  zabbix_server_port="0";
                          String  zabbix_server_name="default";
                          
                          if (nNode.hasAttributes()){

                                NamedNodeMap  baseElmnt_attr = nNode.getAttributes();
                                for (int i = 0; i <  baseElmnt_attr.getLength(); ++i)
                                {
                                    Node attr =  baseElmnt_attr.item(i);

                                    if (attr.getNodeName().equalsIgnoreCase("host")){
                                        itemhost =  nNode.getAttributes().getNamedItem("host").getNodeValue();
                                    } else if (attr.getNodeName().equalsIgnoreCase("prefix")){
                                        prefix2 =   nNode.getAttributes().getNamedItem("prefix").getNodeValue();
                                    } else if (attr.getNodeName().equalsIgnoreCase("zabbix_server")){
                                        zabbix_server_name  =   nNode.getAttributes().getNamedItem("zabbix_server").getNodeValue();
                                    } else if (attr.getNodeName().equalsIgnoreCase("active")){
                                        active =   nNode.getAttributes().getNamedItem("active").getNodeValue();
                                    } 
                           
                                }
                        } else {
                        }  
                          
                         boolean isZabbixServer = false;

                         for (ZabbixServerApi zabbix_server1 : zabbixServerApiList) {
                               if (zabbix_server1.getServerName().equalsIgnoreCase(zabbix_server_name)) {
                                   zabbix_server_host=zabbix_server1.getHost();
                                   zabbix_server_port=zabbix_server1.getStringPort();
                                   isZabbixServer = true;
                                   break;
                               }
                          }  

                         if (!isZabbixServer){
                              zabbix_server_host="localhost";
                              zabbix_server_port="10500";
                              zabbix_server_name="not_exist";
                              logger.error("!isZabbixServer " + zabbix_server_name);
                          }
                          
                      
                          Element eElementMain = (Element) nNode;
                          NodeList  nListItem = eElementMain.getElementsByTagName("item");
                                              
                          for (int it = 0; it < nListItem.getLength(); it++) {
                               Node nNodeItem = nListItem.item(it);
                                if (nNodeItem.getNodeType() == Node.ELEMENT_NODE) {
                                     Element eElementItem = (Element) nNodeItem;
                                     
                                     String itemKey = "default";
                                                            
                                     SybaseApi sybaseApi = new SybaseApi();
                                     
                                     NamedNodeMap attributes = eElementItem.getAttributes();
                                     for (int i = 0; i <  attributes .getLength(); ++i) {
                                          Node item = attributes.item(i);
                                          if (item.getNodeName().equals("key")){
                                               itemKey = item.getNodeValue();
                                          } else if (item.getNodeName().equals("active")){
                                               String tmp =  item.getNodeValue();
                                               if (tmp.equals("false")){
                                                   sybaseApi.setToInactiveMode();
                                               }
                                          }
                                          
                                    } 
                                     
                                     logger.info("Processing " + zabbix_server_name+"."+itemhost+"."+prefix1+prefix2+itemKey);
                                     
                                     sybaseApi.setAlias(zabbix_server_name+"."+itemhost+"."+prefix1+prefix2+itemKey);
                                     sybaseApi.setZabbixKey(prefix1+prefix2+itemKey);
                                     sybaseApi.setZabbixHost(itemhost);
                                     sybaseApi.setSettingFile(filename);
                                     if (active.equals("false")) sybaseApi.setToInactiveMode();
                                                                         
                                     sybaseApi.setZabbixServerName(zabbix_server_name);
                                 
                                     sqlType type = sqlType.PURE;     
                                    
                                    String _type = "PURE";
                                  
                                    if (eElementItem.getElementsByTagName("sql-query").item(0).hasAttributes()){
                                         try {
                                             _type = eElementItem.getElementsByTagName("sql-query").item(0).getAttributes().getNamedItem("type").getNodeValue().toUpperCase();
                                              type = sqlType.valueOf(_type);
                                         } catch (DOMException e){
                                             logger.error("No enum constant " + _type);
                                         }
                                     } else {
                                         logger.warn("sql-query has not attributes !!!!");
                                     } 

                                   if (logger.isDebugEnabled()) {
                                       logger.debug("Query type " + type.toString());
                                   }
                                   
                                   if (type != sqlType.SYSMON && type != sqlType.SYSMON_WITH_DISCOVERY && type != sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED ){
                                       sybaseApi.setSqlType(type); 
                                       String cron_expression = "";
                                       
                                           
                                       if (eElementItem.getElementsByTagName("cron_expression").item(0).hasAttributes()){

                                            NamedNodeMap  baseElmnt_attr = eElementItem.getElementsByTagName("cron_expression").item(0).getAttributes();
                                            for (int i = 0; i <  baseElmnt_attr.getLength(); ++i) {
                                                Node attr =  baseElmnt_attr.item(i);
                                                logger.debug(attr.getNodeName() + " = " + attr.getNodeValue());
                                                if (attr.getNodeName().equals("group") && attr.getNodeValue().equalsIgnoreCase("true")){
                                                    String group = getTagValue("cron_expression", eElementItem);
                                                    if (CrontabGroupsList.containsKey(group)){
                                                        cron_expression = CrontabGroupsList.get(group);
                                                        logger.debug("Set crontab expression " + cron_expression + " -> " + group);
                                                    } else {
                                                        sybaseApi.addErrorToConfigErrorList("Crontab group not found " + group);
                                                        logger.error("Crontab group not found " + group);
                                                        cron_expression = "0 0 * * * ?";
                                                    }
                                                }

                                            }
                                     }  else {
                                         cron_expression = getTagValue("cron_expression", eElementItem);
                                     }
                                    
                                        
                                        sybaseApi.setCronExpression(cron_expression);
                                   } else {
                                       sybaseApi.setSqlType(type);
                                       sybaseApi.setSqlQuery("SELECT 1");
                                   }
                                  
                                                               
                                   if (type != sqlType.MAINTENANCE && type != sqlType.SYSMON && type != sqlType.SYSMON_WITH_DISCOVERY  && type != sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED && type != sqlType.COMPARE){
                                          boolean isJdbcSource = false;

                                          for (JdbcApi jdbc_sybase1 :  JdbcApiList) {
                                               if (jdbc_sybase1.getInterfaceName().equalsIgnoreCase(getTagValue("sybase_jdbc", eElementItem))) {
                                                   sybaseApi.setJdbcDriver(jdbc_sybase1);
                                                   isJdbcSource = true;
                                                   break;
                                               }
                                           }

                                          if (!isJdbcSource){
                                              //BUG 20171119
                                              sybaseApi.addErrorToConfigErrorList("unknown JdbcSource " + getTagValue("sybase_jdbc", eElementItem));
                                              logger.error("!isJdbcSource " + getTagValue("sybase_jdbc", eElementItem));
                                              System.exit(5);
                                          }
                                   }
                                   
                                   if (type == sqlType.SYSMON || type == sqlType.SYSMON_WITH_DISCOVERY  || type == sqlType.SYSMON_WITH_DISCOVERY_EMBEDDED ){
                                       sybaseApi.setSysmonName(getTagValue("sysmon_name", eElementItem));
                                       boolean isSysmon=false;
                                       for (SysmonApi sysmonApi : sysmonApiList){
                                           if (sysmonApi.getName().equalsIgnoreCase(getTagValue("sysmon_name", eElementItem))){
                                               isSysmon=true;
                                               //BUG 20171119
                                               sybaseApi.setCronExpression(sysmonApi.getCronExpression());
                                               sybaseApi.addErrorToConfigErrorList(sysmonApi.getErrorList());
                                               break;
                                           }
                                       }
                                       if (!isSysmon) {
                                           sybaseApi.addErrorToConfigErrorList("unknown sysmon " + getTagValue("sysmon_name", eElementItem));
                                           logger.error("Sysmon " + getTagValue("sysmon_name", eElementItem) + " not found!!!!");
                                       }
                                   } 

                                   if (logger.isDebugEnabled()) logger.debug("sql-param size " + eElementItem.getElementsByTagName("sql-param").getLength());
                                    
                                   for (int count = 0; count < eElementItem.getElementsByTagName("sql-param").getLength(); count++) {
                                         
                                                     
                                          if (eElementItem.getElementsByTagName("sql-param").item(count).hasAttributes()){

                                                NamedNodeMap  baseElmnt_attr = eElementItem.getElementsByTagName("sql-param").item(count).getAttributes();
                                                for (int i = 0; i <  baseElmnt_attr.getLength(); ++i)
                                                {
                                                    Node attr =  baseElmnt_attr.item(i);

                                                    if (attr.getNodeName().equalsIgnoreCase("key")){
                                                        logger.debug(attr.getNodeValue() + " = " + eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        if (attr.getNodeValue().equalsIgnoreCase("fullquery")) {
                                                            sybaseApi.setFullQuery(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("fireatstartup")) {
                                                            sybaseApi.setFireAtStartup(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("sendtozabbix")) {
                                                            sybaseApi.setSentToZbx(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("string")) {
                                                            sybaseApi.setIsString(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("numeric")) {
                                                            sybaseApi.setIsNumeric(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("integer")) {
                                                            sybaseApi.setIsInteger(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("float")) {
                                                            sybaseApi.setIsFloat(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("long")) {
                                                            sybaseApi.setIsLong(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("column")) {
                                                            sybaseApi.setColumn(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("row")) {
                                                            sybaseApi.setRow(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("timing")) {
                                                            sybaseApi.setIsTiming(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("show_count")) {
                                                            sybaseApi.setShowCount(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("html")) {
                                                            sybaseApi.setHtml(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("more_stmt")) {
                                                            sybaseApi.setMoreStmt(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("isolation_level")) {
                                                            sybaseApi.setIsolationLevel(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("no_data")) {
                                                            sybaseApi.setNoData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("complementary")) {
                                                            sybaseApi.setComplementary_query(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("complementaryMapAndMetadata")) {
                                                            sybaseApi.setComplementaryMapAndMetaData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryComplementary")) {
                                                            long delay=1000;
                                                            try {
                                                               delay=Long.parseLong(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                            } catch (Exception iii){}
                                                            sybaseApi.setComplementaryUpdateInterval(delay);
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("discovery")) {
                                                            sybaseApi.setDiscoveryQuery(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryMapAndMetadata")) {
                                                            sybaseApi.setDiscoveryMapAndMetaData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryMetadata")) {
                                                            sybaseApi.setDiscoveryMetaData(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("discoveryMap")) {
                                                            sybaseApi.setDiscoveryMap(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }  else if (attr.getNodeValue().equalsIgnoreCase("discoveryParams")) {
                                                            sybaseApi.setDiscoveryParamsType(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        } else if (attr.getNodeValue().equalsIgnoreCase("discoveryUpdate")) {
                                                            long delay=1000;
                                                            try {
                                                               delay=Long.parseLong(eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                            } catch (Exception iii){}
                                                            sybaseApi.setDiscoveryUpdateInterval(delay);
                                                        }   else {                                                        
                                                            sybaseApi.setDatabaseProperty(attr.getNodeValue(), eElementItem.getElementsByTagName("sql-param").item(count).getTextContent());
                                                        }
                                                    } else {
                                                        sybaseApi.addErrorToConfigErrorList("unknown attribute " + attr.getNodeName());
                                                        logger.error("unknown attribute " + attr.getNodeName());
                                                    }

                                                }
                                          } else {
                                               sybaseApi.addErrorToConfigErrorList("sql-param has not attributes");
                                                logger.error("sql-param has not attributes !!!!");
                                          } 
                                    }

                                   String _query = "default";

                                   switch (type) {  
                                        case PURE:  
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break; 
                                        
                                        case DB_MAINTENANCE:  
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break; 
                                      
                                        case PURE_WITH_MULTIPLE:  
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;  
                                        
                                        case MAINTENANCE:  
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 sybaseApi.setSybaseInterface("");
                                                 break; 
                                        
                                   
                                        case DISCOVERY_QUERY: 
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;  
                                         
                                        case DISCOVERY_STATIC: 
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;    
                                        
                                        case DISCOVERY_EMBEDDED: 
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;
                                      
                                        case EMBEDDED: 
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break; 
                                    
                                        case EMBEDDED_WITH_DISCOVERY: 
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break; 
                                        
                                        case EMBEDDED_WITH_DISCOVERY_EMBEDDED: 
                                                 _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break; 
                                     
                                        case SYSMON: 
                                                _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;
                                        
                                        case SYSMON_WITH_DISCOVERY: 
                                                _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;
                                        
                                        case SYSMON_WITH_DISCOVERY_EMBEDDED : 
                                                _query = getTagValue("sql-query", eElementItem);
                                                 sybaseApi.setSqlQuery(_query);
                                                 break;
                                                 
                                        case PREDEFINED: 
                                        case PREDEFINED_WITH_MULTIPLE: 
                                                boolean isqueryType = false;
                                                _query = getTagValue("sql-query", eElementItem);
                                                for (QueryApi query1 : queryList) {
                                                      if (query1.getName().equalsIgnoreCase(getTagValue("sql-query", eElementItem))) {
                                                          _query = query1.getQuery();
                                                          String _param = query1.getQueryProperty("txt_params", "NO");
                                                          String _params [] = _param.split(",");
                                                          //podmiana w zapytaniu
                                                          if (!_param.equalsIgnoreCase("NO")){
                                                              for (String param : _params) {
                                                                  String value = sybaseApi.getDatabaseProperty(param, "NO_SETTING_VALUE");
                                                                  if (value.equalsIgnoreCase("NO_SETTING_VALUE")){
                                                                      logger.warn("NO_SETTING_VALUE for " + param);
                                                                  } else {
                                                                      _query = _query.replaceAll(param, value);
                                                                  }
                                                              }
                                                          }
                                                          logger.info("full query: " + _query);
                                                          sybaseApi.setSqlQuery(_query); 
                                                          
                                                          //po nowemu
                                                          Set<Object> keys = query1.getQueryProperties().keySet();
                                                          for(Object k:keys){
                                                                 String key = (String)k;
                                                                 boolean isKey=false;
                                                                 logger.debug(key + " = " + query1.getQueryProperties().getProperty(key));
                                                                 if (key.equalsIgnoreCase("fullquery")) {
                                                                        sybaseApi.setFullQuery(query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                 } else if (key.equalsIgnoreCase("fireatstartup")) {
                                                                        sybaseApi.setFireAtStartup(query1.getQueryProperties().getProperty(key));
                                                                        isKey=true; 
                                                                 } else if (key.equalsIgnoreCase("sendtozabbix")) {
                                                                        sybaseApi.setSentToZbx(query1.getQueryProperties().getProperty(key));
                                                                        isKey=true; 
                                                                 } else if (key.equalsIgnoreCase("string")) {
                                                                        sybaseApi.setIsString(query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                 } else if (key.equalsIgnoreCase("numeric")) {
                                                                        sybaseApi.setIsNumeric( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                 } else if (key.equalsIgnoreCase("integer")) {
                                                                        sybaseApi.setIsInteger( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("float")) {
                                                                        sybaseApi.setIsFloat( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("long")) {
                                                                        sybaseApi.setIsLong( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("column")) {
                                                                        sybaseApi.setColumn( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("row")) {
                                                                        sybaseApi.setRow( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("timing")) {
                                                                        sybaseApi.setIsTiming( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("more_stmt")) {
                                                                        sybaseApi.setMoreStmt( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("isolation_level")) {
                                                                        sybaseApi.setIsolationLevel( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("no_data")) {
                                                                        sybaseApi.setNoData( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("show_count")) {
                                                                        sybaseApi.setShowCount(query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("html")) {
                                                                        sybaseApi.setHtml(query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("discovery")) {
                                                                       sybaseApi.setDiscoveryQuery( query1.getQueryProperties().getProperty(key));
                                                                       isKey=true;
                                                                } else if (key.equalsIgnoreCase("discoveryMapAndMetadata")) {
                                                                        sybaseApi.setDiscoveryMapAndMetaData( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("discoveryMetadata")) {
                                                                        sybaseApi.setDiscoveryMetaData( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("discoveryMap")) {
                                                                        sybaseApi.setDiscoveryMap( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                }  else if (key.equalsIgnoreCase("discoveryParams")) {
                                                                        sybaseApi.setDiscoveryParamsType( query1.getQueryProperties().getProperty(key));
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("discoveryUpdate")) {
                                                                        long delay=1000;
                                                                        try {
                                                                           delay=Long.parseLong( query1.getQueryProperties().getProperty(key));
                                                                        } catch (Exception iii){}
                                                                        sybaseApi.setDiscoveryUpdateInterval(delay);
                                                                        isKey=true;
                                                                } else if (key.equalsIgnoreCase("txt_params")) {
                                                                        //do nothing
                                                                        isKey=true;
                                                                } else {                                                        
                                                                        sybaseApi.setDatabaseProperty(key,  query1.getQueryProperties().getProperty(key));
                                                                } 
                                                                if (!isKey){
                                                                    logger.debug("Set database property " + key + " = " + query1.getQueryProperties().getProperty(key));
                                                                }
                                                            }
                                                        
                                                         // sybaseApi.setExecute(query1.getQueryProperty("only_execute", "false"));
                                                         // sybaseApi.setIsNumeric(query1.getQueryProperty("numeric", "true"));
                                                         // sybaseApi.setColumn(query1.getQueryProperty("column", "1"));
                                                         // sybaseApi.setRow(query1.getQueryProperty("row", "1"));
                                                         // sybaseApi.setIsTiming(query1.getQueryProperty("timing", "false"));
                                                         // sybaseApi.setMoreStmt(query1.getQueryProperty("more_stmt", "1"));
                                                         //  sybaseApi.setIsolationLevel(query1.getQueryProperty("isolation_level", "-1")); //default -1
                                                         isqueryType = true;
                                                         break;
                                                      }
                                                  }     
                                                  if (!isqueryType)  {
                                                      sybaseApi.addErrorToConfigErrorList("queryType for " + getTagValue("sql-query", eElementItem) + " not found");
                                                      logger.error("!isqueryType for " + getTagValue("sql-query", eElementItem));
                                                  }

                                                  break;
                                        case COMPARE: 
                                              boolean isJdbcSource1 = false;
                                              boolean isJdbcSource2 = false;

                                              for (JdbcApi jdbc_sybase1  :  JdbcApiList) {
                                                   if (jdbc_sybase1.getInterfaceName().equalsIgnoreCase(getTagValue("sybase_jdbc_master", eElementItem))) {
                                                       sybaseApi.setDatabaseProperty("sybase_host_master", jdbc_sybase1.getHost());
                                                       sybaseApi.setDatabaseProperty("sybase_port_master", jdbc_sybase1.getStringPort());
                                                       sybaseApi.setDatabaseProperty("sybase_user_master", jdbc_sybase1.getUser());
                                                       sybaseApi.setDatabaseProperty("sybase_password_master", jdbc_sybase1.getPassword());
                                                       isJdbcSource1 = true;
                                                       break;
                                                   }
                                               }

                                              if (!isJdbcSource1){
                                                  sybaseApi.setDatabaseProperty("sybase_host_master", "0.0.0.0");
                                                  sybaseApi.setDatabaseProperty("sybase_port_master", "0");
                                                  sybaseApi.setDatabaseProperty("sybase_user_master", "usr");
                                                  sybaseApi.setDatabaseProperty("sybase_password_master", "pwd");
                                                  sybaseApi.addErrorToConfigErrorList("!isJdbcSource1 " + getTagValue("sybase_jdbc_master", eElementItem));
                                                   logger.error("!isJdbcSource1 " + getTagValue("sybase_jdbc_master", eElementItem));
                                              }  
                                              
                                              for (JdbcApi jdbc_sybase2  :  JdbcApiList) {
                                                   if (jdbc_sybase2.getInterfaceName().equalsIgnoreCase(getTagValue("sybase_jdbc_slave", eElementItem))) {
                                                       sybaseApi.setDatabaseProperty("sybase_host_slave", jdbc_sybase2.getHost());
                                                       sybaseApi.setDatabaseProperty("sybase_port_slave", jdbc_sybase2.getStringPort());
                                                       sybaseApi.setDatabaseProperty("sybase_user_slave", jdbc_sybase2.getUser());
                                                       sybaseApi.setDatabaseProperty("sybase_password_slave", jdbc_sybase2.getPassword());
                                                       isJdbcSource2 = true;
                                                       break;
                                                   }
                                               }

                                              if (!isJdbcSource2){
                                                  sybaseApi.setDatabaseProperty("sybase_host_slave", "0.0.0.0");
                                                  sybaseApi.setDatabaseProperty("sybase_port_slave", "0");
                                                  sybaseApi.setDatabaseProperty("sybase_user_slave", "usr");
                                                  sybaseApi.setDatabaseProperty("sybase_password_slave", "pwd");
                                                  sybaseApi.addErrorToConfigErrorList("!isJdbcSource2 " + getTagValue("sybase_jdbc_slave", eElementItem));
                                                   logger.error("!isJdbcSource2 " + getTagValue("sybase_jdbc_slave", eElementItem));
                                              }
                                                
                                          
                                              sybaseApi.setDatabaseProperty("sql-query_master", getTagValue("sql-query_master", eElementItem));
                                              sybaseApi.setDatabaseProperty("sql-query_slave", getTagValue("sql-query_slave", eElementItem));
                                              
                                              isqueryType = true;
                                                 break;
                                        default: //;  
                                            sybaseApi.setSqlQuery("SELECT -2");
                                            sybaseApi.addErrorToConfigErrorList("Unknown query type");                                           
                                            logger.error("Unknown query type");
                                            break;
                                    }
                                   sybaseApiList.add(sybaseApi);
                                   WorkingStats.initLockForQueue();
                                }
                                
                                
                          }
                          
                          
                          
                         
                         
                         
                         
                    
		   }
		}
                
                logger.info("Read sybase-to-zabbix from " + filename + " done");
       
                
         }  catch (ParserConfigurationException | SAXException | IOException | NumberFormatException e) {            
                logger.fatal("sybase-to-zabbix::XML Exception/Error:", e);
                System.exit(-1);
				
	  }
    } 
 
  
   private static String getTagValue(String sTag, Element eElement) {
	
        try {
            NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
            Node nValue = (Node) nlList.item(0);
            return nValue.getNodeValue();
        } catch (Exception e){
            logger.error("getTagValue error " + sTag + " "+ e);
            return "ERROR";
        }

	
}    
   
    public Set<Object> getAllKeys(Properties prop){
        Set<Object> keys = prop.keySet();
        return keys;
    }

    public String getPropertyValue(Properties prop, String key){
        return prop.getProperty(key);
    }
  

    public List<SybaseApi> getSybaseApiList(){
          return sybaseApiList;
    }
 
}