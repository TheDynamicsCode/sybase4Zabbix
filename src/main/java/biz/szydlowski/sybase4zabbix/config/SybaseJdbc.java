/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix.config;

import biz.szydlowski.sybase4zabbix.api.JdbcApi;
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
 * @author dominik
 */
public class SybaseJdbc {
 
   private List<JdbcApi> jdbcApiList = new ArrayList<>();
    
   private String _setting="setting/sybase-jdbc.xml";
   
    static final Logger logger =  LogManager.getLogger("biz.szydlowski.sybase4zabbix.config.SybaseJdbc");

       
     /** Konstruktor pobierający parametry z pliku konfiguracyjnego "config.xml"
     */
     public  SybaseJdbc (){
         
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
             
		logger.debug("Read sybase-jdbc " + _setting);
              
                NodeList  nList = doc.getElementsByTagName("sybase");
                int pool_index=0;	 
		for (int temp = 0; temp < nList.getLength(); temp++) {
                
		   Node nNode = nList.item(temp);
		   if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                          Element eElement = (Element) nNode;
                                                
                          JdbcApi jdbcApi = new JdbcApi();
                          
                          jdbcApi.setInterfaceName( getTagValue("interface_name", eElement));
                          jdbcApi.setInterfaceType( getTagValue("interface_type", eElement));
                          
                          if ( getTagValue("interface_type", eElement).equalsIgnoreCase("single") ){
                               jdbcApi.setConnectionRetry(getTagValue("connectionRetryCount", eElement));
                               jdbcApi.setLoginTimeout(getTagValue("loginTimeout", eElement));
                          } else {
                               jdbcApi.setPoolIndex(pool_index);
                               pool_index++;
                          }
                          
                          for (int count = 0; count < eElement.getElementsByTagName("pool-param").getLength(); count++) {
                                 if (eElement.getElementsByTagName("pool-param").item(count).hasAttributes()){

                                        NamedNodeMap  baseElmnt_attr = eElement.getElementsByTagName("pool-param").item(count).getAttributes();
                                        for (int i = 0; i <  baseElmnt_attr.getLength(); ++i)
                                        {
                                            Node attr =  baseElmnt_attr.item(i);

                                            if (attr.getNodeName().equalsIgnoreCase("key")){
                                                logger.debug("pool-praram " + attr.getNodeValue() + " = " + eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                if (attr.getNodeValue().equals("minPoolSize")){
                                                   jdbcApi.setMinPoolSize(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("maxPoolSize")){
                                                   jdbcApi.setMaxPoolSize(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("maxStatements")){
                                                   jdbcApi.setMaxStatements(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("acquireIncrement")){
                                                   jdbcApi.setAcquireIncrement(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                } else  if (attr.getNodeValue().equals("loginTimeout")){
                                                   jdbcApi.setLoginTimeout(eElement.getElementsByTagName("pool-param").item(count).getTextContent());
                                                }
                                            } else {
                                                logger.error("unknown attribute " + attr.getNodeName());
                                            }

                                        }
                                  } else {
                                        logger.error("sql-param has not attributes !!!!");
                                  }
                          }
                          
                          jdbcApi.setGroup(getTagValue("group", eElement));
                          jdbcApi.setHost(getTagValue("host", eElement));
                          jdbcApi.setPort(getTagValue("port", eElement));
                          jdbcApi.setUser(getTagValue("user", eElement));
                          jdbcApi.setPassword(getTagValue("password", eElement));
                          
                          jdbcApiList.add(jdbcApi);
		   }
		}
                
                logger.debug("Read sybase-jdbc done");
                                
         }  catch (ParserConfigurationException | SAXException | IOException e) {         
                logger.fatal("sybase-jdbc.xml::XML Exception/Error:", e);
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
  
  public  List<JdbcApi> getJdbcApiList(){
          return  jdbcApiList;
  }

}
