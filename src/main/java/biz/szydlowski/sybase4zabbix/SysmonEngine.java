/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.szydlowski.sybase4zabbix;


import biz.szydlowski.sybase4zabbix.api.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author dominik
 */
public class SysmonEngine {
  
     static final Logger logger =  LogManager.getLogger(SysmonEngine.class);
    private int sample_interval_ms = 4000;
       
    private Connection conn = null; 
    
    private ArrayList<ArrayList<String>> metadata_helper = new ArrayList<>();
  
    private ArrayList<ArrayList<String>> threads_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> threads_end = new ArrayList<>();
    private ArrayList<ArrayList<String>> threads_diff = new ArrayList<>();
    private ArrayList<ArrayList<String>> threads_info = new ArrayList<>(); 
    private ArrayList<ArrayList<Integer>> engine_info = new ArrayList<>();
    private ArrayList<ArrayList<String>> tmpLoad = new ArrayList<>();
    private ArrayList<ArrayList<String>> sp_monitorconfig = new ArrayList<>();
       
    private ArrayList<ArrayList<String>> threads_pool_maps = new ArrayList<>(); 
       
    private ArrayList<ArrayList<String>> sysmonitors_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> sysmonitors_end = new ArrayList<>();
    private ArrayList<ArrayList<String>> sysmonitors_diff = new ArrayList<>();
    private ArrayList<ArrayList<String>> sp_cluster_show = new ArrayList<>();
    private ArrayList<ArrayList<String>> sp_lock = new ArrayList<>();
    
    private ArrayList<ArrayList<String>> workload_data1 = new ArrayList<>();
    private ArrayList<ArrayList<String>> workload_data2 = new ArrayList<>();
    private ArrayList<ArrayList<String>> workload_data3 = new ArrayList<>();
      
    private ArrayList<Double> engineBusy_cpu_use = new ArrayList<> ();
    private ArrayList<Double> engineBusy_io_use = new ArrayList<> ();
    private ArrayList<Double> engineBusy_system_use = new ArrayList<> ();
    private ArrayList<Double> engineBusy_idle_use = new ArrayList<> ();
    private ArrayList<Double> engineBusy_user_use = new ArrayList<> (); 
    
    private ArrayList<ArrayList<String>> monProcedureCache_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> monProcedureCache_end = new ArrayList<>();
    
    private ArrayList<ArrayList<String>> monDataCache_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> monDataCache_end = new ArrayList<>();
    
    private ArrayList<ArrayList<String>> monIOQueue_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> monIOQueue_end = new ArrayList<>();
    
    private ArrayList<ArrayList<String>> monNetworkIO_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> monNetworkIO_end = new ArrayList<>();
    
    private long max_outstanding_AIOs_server;
    private ArrayList<ArrayList<String>> max_outstanding_AIOs_engine_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> max_outstanding_AIOs_engine_end = new ArrayList<>();
    private ArrayList<ArrayList<String>> sysdevices = new ArrayList<>();
    private ArrayList<ArrayList<String>> sysdevices_io_begin = new ArrayList<>();
    private ArrayList<ArrayList<String>> sysdevices_io_end = new ArrayList<>();
      
    private ArrayList<Double>  CPUYields_cnt = new ArrayList<> ();
    private ArrayList<Double>  CPUYields_cnt_interrupted = new ArrayList<> ();
    
    private  Map<String, Integer> Locks = new HashMap<>();
    
    private  ArrayList<Integer>  engine_maps = new ArrayList<> ();  
    private  ArrayList<ArrayList<String>>  engine_pool_maps = new ArrayList<> (); 
    private ArrayList<String> cols = new ArrayList<>();
 
    private double CPUYields_sum = 0.0;
    private double CPUYields_sum_interrupted = 0.0;
    private double TotalRequestedDiskIOs=0.0;
    private double TotalNetworkIORequests=0.0;
    
    private int version_number = 0;
    private String version_str = "";
    private int col_field_name_array = -1;
    private int col_group_name_array = -1;
    //private int col_field_id_array = -1;
    private int col_value_array=-1;
    //private int col_description_array=-1;
    private int col_instanceid_array=-1;
   // private int col_nodeid_array=-1;
    
    private int clockTicks = 0;
    private int NumEngines = 1;
    private int timeticks=1;
    private int seconds=0;
    private int miliseconds=0;
    private String system_view="sw";
    private String kernelmode=""; 
    private int setInstanceid=-1;
    private double threadCpuUse=0.0;
    private boolean isCluster=false;
            
    private boolean isEngineError=false;
    private boolean isErrorInTest=false;
    private long executeTime;
    private long calculateTime;
    private int hits=4;
    private boolean use_monitorTable=false;
    private int negative=0;
            
    private  String type;
    private  int idpool; 
    private  String sybase_host;
    private int sybase_port;
    private  String sybase_user; 
    private String sybase_password;
    private int timeout;
    
    private long Disk_io_structures; 
    private long AIOs_delayed_due_to_server_limit; 
    private long AIOs_delayed_due_to_engine_limit;
    private long AIOs_delayed_due_to_os_limit;
    private long total_async; 
    private long total_sync; 
    private boolean first=true;
    private boolean isInstanceid=false;
    private boolean isNodeid=false;
    
    //--sp_sysmon '00:00:05', kernel  
    
   
    private  DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.GERMAN);
     
    private String returnString(double v){
        otherSymbols.setDecimalSeparator('.');
        otherSymbols.setGroupingSeparator('.'); 
        DecimalFormat df = new DecimalFormat("###.######", otherSymbols);
        return df.format(v);
    }
  
    public  SysmonEngine (int sampletime){
       sample_interval_ms = sampletime;
       isEngineError=false;
       logger.info("***** CREATE SysmonEngine, engine version 4.20190730 ********");       
    }
    
    public boolean isEngineError(){
        return isEngineError;
    } 
    
    public boolean isErrorInTest(){
        return isErrorInTest;
    }
                    
    public void initSysmon(boolean use_monitorTable, String type, int idpool, String sybase_host, int sybase_port, String sybase_user, String sybase_password, int timeout, int retry){ 
       
        logger.info("***** INIT SysmonEngine ******** " + sybase_host);
         
        this.type=type;
        this.idpool=idpool; 
        this.sybase_host=sybase_host;
        this.sybase_port=sybase_port;
        this.sybase_user=sybase_user; 
        this.sybase_password=sybase_password;
        this.timeout=timeout;
        this.use_monitorTable=use_monitorTable;
            

        int licz_conn=0;
       
        while (licz_conn<=retry){
           conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
           
           if (conn==null){
                logger.error("Conn in sysmonEngine is null RETRY " + licz_conn);  
                try {
                   Thread.sleep(1000);
               } catch (InterruptedException ex) {
                   logger.error(ex);
               }
                licz_conn++;
            } else {
                licz_conn=retry+1;
                break;
            }
        }
        
        if (conn==null){
            logger.error("Conn in sysmon is null");
            isEngineError=true;
            return;
       }
       
        if (type.equalsIgnoreCase("pool")) attemptClose(conn);
        
        if (first){
            
             execQuery("sp_sysmon '00:00:05', kernel");
            
            logger.info("FIRST RUN SYSMON ENGINE for " + sybase_host);
            
            try {
                version_number = Integer.parseInt(doQuery("select  @@version_number"));
             } catch (NumberFormatException e){

            }
            
            version_str = doQuery("select  @@version");
            insertQueryToArrayOnlyMetaData(conn, "select top 1 * from master..sysmonitors", metadata_helper, true);
            isInstanceid = isStringInMetadataHelper("instanceid");
            isNodeid = isStringInMetadataHelper("nodeid");
            
          
            
            if (version_number>=15700){   
                
                if (version_str.contains("Cluster")) {
                    // System.out.println("Cluster");
                     isCluster=true;
                } 
                      
                           
                if (isCluster) {
                   
                    try {
                         setInstanceid = Integer.parseInt(doQuery("select @@instanceid").replaceAll("\\s+", ""));
                    } catch (NumberFormatException e){
                         logger.error("setInstanceid " + e);
                    }
            
            
                }  else setInstanceid = 0;
               
               logger.debug("set auto setInstanceid to " + setInstanceid);
                             
             
                
               insertQueryToArrayWithoutMetaData(conn, "select InstanceID,  ThreadPoolID, ThreadPoolName from master..monThread where instanceid=" + setInstanceid + " group by InstanceID, ThreadPoolName, ThreadPoolID", threads_pool_maps,  true);  
               insertQueryToArrayWithoutMetaData(conn, "select me.EngineNumber, ThreadPoolID from  master..monEngine me, master..monThread mt where me.ThreadID=mt.ThreadID and mt.InstanceID=" + setInstanceid, engine_pool_maps, true);  
                                
                  
               if (isCluster){ 
                    system_view = doQuery("select @@system_view").replaceAll("\\s+", "");
                    NumEngines = Integer.parseInt(doQuery("select count(*) from sysengines where status='online' and instanceid=" + setInstanceid)); 
                } else {
                    NumEngines = Integer.parseInt(doQuery("select count(*) from master..sysengines where status='online'"));
                }              
               
                kernelmode  = doQuery("select @@kernelmode").replaceAll("\\s+", "");
                 
                if (isCluster) insertQueryToIntegerColumn(conn, "select engine as 'ENGINE' from master..sysengines where status='online' and instanceid=" + setInstanceid, engine_maps);  
                else  insertQueryToIntegerColumn(conn, "select engine as 'ENGINE' from master..sysengines where status='online'", engine_maps);  
                 
                         
                if (kernelmode.equalsIgnoreCase("threaded")){
            
                    insertQueryToArrayWithoutMetaData(conn, "select distinct ThreadID, name, instanceID from master..monTask where instanceID="+ setInstanceid+" and name in ('CIPC Controller', 'DiskController', 'IP Link Monitor', 'NetController',  'Signal Handler') group by instanceid, name", threads_info, false);
                    insertQueryToIntegerArray(conn, "select ThreadID, EngineNumber, Connections from master..monEngine where instanceID=" + setInstanceid, engine_info);

                }  
                
                insertQueryToArrayWithoutMetaData(conn, "select name, 'disk_'+convert(char,vdevno) from master..sysdevices where cntrltype=0",  sysdevices, true);           
                
                     
            }  else {   
                      
                    setInstanceid = 0;
              
                
                    insertQueryToArrayWithoutMetaData(conn, "select name, 'disk_'+convert(char(3),((low&-16777216)/16777216 &255)) from master..sysdevices where (status & 16) != 16",  sysdevices, true);           
          

                    insertQueryToIntegerColumn(conn, "select engine as 'ENGINE' from master..sysengines where status='online'", engine_maps);  
                    NumEngines = Integer.parseInt(doQuery("select count(*) from master..sysengines where status='online'"));

            }
         
           
        
            first=false;
            
        } //end first
      
   
         executeTime = System.currentTimeMillis();        
     
       
        try {
            negative  = Integer.parseInt(doQuery("select count(*) from master..sysmonitors where value<0"));
            if (negative> 50){
               logger.info("*** clearSample ***");
               execQuery("sp_sysmon '00:00:01', 'kernel', 'clear'");
            }
        } catch (NumberFormatException e){
            
        }
                     
        
           
        if (version_number>=15700){    
        
           
            if (isCluster) {
                insertQueryToArrayWithoutMetaData(conn, "sp_cluster show", sp_cluster_show, true);   

                if (system_view.equalsIgnoreCase("cluster")){

                   insertQueryToArrayWithoutMetaData(conn, "select InstanceID, ConnectionsRaw, CpuRaw, RunQueueRaw, IoLoadRaw, EngineRaw, UserRaw from monWorkloadRaw order by InstanceID", workload_data1, true);
                   insertQueryToArrayWithoutMetaData(conn, "select wml.InstanceID,  wml.LogicalClusterName, wml.LoadProfileName, lc.ActiveConnections, lc.State, wml.LoadScore, wml.ConnectionsScore, wml.CpuScore, wml.RunQueueScore, wml.IoLoadScore, wml.EngineScore, wml.UserScore from monWorkload wml, monLogicalCluster lc where wml.LCID = lc.LCID order by wml.InstanceID", workload_data2, true); 
                   insertQueryToArrayWithoutMetaData(conn, "select distinct mwp.InstanceID, mwp.LoadProfileName, mwp.LoadScore, mwp.ConnectionsScore, mwp.CpuScore, mwp.RunQueueScore, mwp.IoLoadScore, mwp.EngineScore, mwp.UserScore from monWorkload mwk, monWorkloadPreview mwp, monWorkloadRaw mwr where mwr.InstanceID = mwp.InstanceID order by mwp.InstanceName", workload_data3, true);
                }

                hits=hits+7;
            }
        }
        
        
        insertQueryToArrayWithoutMetaData(conn, "sp_lock", sp_lock,  true); 
        insertQueryToArrayWithMetaData(conn, "sp_monitorconfig 'all'", sp_monitorconfig,  true); //add metadata=true
        
        
        logger.debug("SYBASE_VER " + version_number + " system view: " + system_view + " : instanceID " + setInstanceid + " : kernelMode " + kernelmode + " idpool " +  idpool + " conn " + sybase_host +  ":" + sybase_port);
        
        getSample();
        
        if (use_monitorTable){
             if (version_number>=15700){  
                insertQueryToArrayWithoutMetaData(conn, "select BytesSentMB, BytesReceivedMB from  master..monNetworkIO  where InstanceID=" + setInstanceid,  monNetworkIO_begin,  false);
                insertQueryToArrayWithoutMetaData(conn, "select LogicalName, sum(IOs), sum(IOTime) from  master..monIOQueue group by LogicalName",  monIOQueue_begin,  false);
                insertQueryToArrayWithoutMetaData(conn, "select * from master..monDataCache where InstanceID="+ setInstanceid, monDataCache_begin,  false);
                insertQueryToArrayWithoutMetaData(conn, "select * from master..monProcedureCache where InstanceID="+ setInstanceid, monProcedureCache_begin, false);
                    
             }             
        }        
     
        
        if (kernelmode.equalsIgnoreCase("threaded")){
            
               
           insertQueryToArrayWithoutMetaData(conn, "select th.InstanceID, th.ThreadID, th.ThreadPoolID, th.MinorFaults, th.MajorFaults, th.UserTime, th.SystemTime, th.TotalTicks, th.IdleTicks, th.SleepTicks, th.BusyTicks, th.TaskRuns," +
            "th.VoluntaryCtxtSwitches, th.NonVoluntaryCtxtSwitches from master..monThread th, master..monTask tk where th.KTID *= tk.KTID and th.InstanceID *= tk.InstanceID group by th.ThreadID, th.ThreadPoolID, th.InstanceID\n", threads_begin, false);
         

           insertQueryToArrayWithoutMetaData(conn, "select StatisticID, l.EngineNumber, Avg_1min, Avg_5min, " +
                    " Avg_15min, ThreadPoolID from monSysLoad l, monEngine e,  monThread t where l.StatisticID in (4, 5) and l.EngineNumber = e.EngineNumber" +
                    " and e.ThreadID = t.ThreadID and l.InstanceID = "  + setInstanceid + " and e.InstanceID = " + setInstanceid +" and t.InstanceID = " + setInstanceid + " order by StatisticID, EngineNumber", tmpLoad, false);
          
            hits=hits+2;
        }
 
        insertQueryToArrayWithoutMetaData(conn, "select * from master..sysmonitors holdlock  where field_name like '%ticks' or field_name like 'udalloc_calls%' or field_name like 'ksalloc_calls%' or field_name in('buf_imdb_privatebuffer_grab', 'engine_sleeps ', 'engine_sleep_interrupted', 'xacts')", sysmonitors_begin, true);
        
  
        if (isInstanceid) {
             
             Disk_io_structures = getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'udalloc_sleeps'  and InstanceID=" + setInstanceid);
             AIOs_delayed_due_to_server_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_server_limit' and InstanceID=" + setInstanceid);
             AIOs_delayed_due_to_engine_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_engine_limit' and InstanceID=" + setInstanceid);
             AIOs_delayed_due_to_os_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_os_limit' and InstanceID=" + setInstanceid);
        
             total_async =getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_dpoll_completed_aios' and InstanceID=" + setInstanceid);
             total_sync =getLongFromQuery("select  isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_sync_completed_ios' and InstanceID=" + setInstanceid); 
             
             max_outstanding_AIOs_server = getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'max_outstanding_AIOs_server' and InstanceID=" + setInstanceid);
      
             insertQueryToArrayWithoutMetaData(conn, "select group_name, field_name, value from master..sysmonitors  where group_name like 'disk_%' and	(field_name = 'total_reads' or field_name = 'total_writes' or field_name ='apf_physical_reads') and InstanceID="+ setInstanceid + " order by field_name, group_name",sysdevices_io_begin, true);
             insertQueryToArrayWithoutMetaData(conn, "select group_name, value from master..sysmonitors where field_name = 'max_outstanding_AIOs_engine' and InstanceID="+ setInstanceid, max_outstanding_AIOs_engine_begin, true);
         }
         else{
             Disk_io_structures = getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'udalloc_sleeps' ");
             AIOs_delayed_due_to_server_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_server_limit' ");
             AIOs_delayed_due_to_engine_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_engine_limit' ");
             AIOs_delayed_due_to_os_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_os_limit' ");
        
             total_async = getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_dpoll_completed_aios'");
             total_sync = getLongFromQuery("select  isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_sync_completed_ios'"); 
            
             max_outstanding_AIOs_server = getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'max_outstanding_AIOs_server'");
       
             insertQueryToArrayWithoutMetaData(conn, "select group_name, field_name, value from master..sysmonitors  where group_name like 'disk_%' and	(field_name = 'total_reads' or field_name = 'total_writes' or field_name ='apf_physical_reads') order by field_name, group_name",sysdevices_io_begin, true);
             insertQueryToArrayWithoutMetaData(conn, "select group_name, value from master..sysmonitors where field_name = 'max_outstanding_AIOs_engine'", max_outstanding_AIOs_engine_begin, true);
        }        
          
         
         
       //**********************************************************************************************************
       
       try {
            logger.debug("Wait sample ms " + sample_interval_ms);
            Thread.sleep(sample_interval_ms);
        } catch (InterruptedException ex) {
            logger.error("e1" + ex);
        }
        getSample();
        
     
        insertQueryToArrayWithoutMetaData(conn, "select * from master..sysmonitors holdlock where field_name like '%ticks' or field_name like 'udalloc_calls%' or field_name like 'ksalloc_calls%' or field_name in('buf_imdb_privatebuffer_grab', 'engine_sleeps ', 'engine_sleep_interrupted', 'xacts')", sysmonitors_end, true);
      
  
        insertQueryToArrayWithMetaData(conn, "select * from master..sysmonitors where field_name='cg_cmaxonline'", sysmonitors_diff, true);
     
      
        if (kernelmode.equalsIgnoreCase("threaded")){
            insertQueryToArrayWithoutMetaData(conn, "select th.InstanceID, th.ThreadID, th.ThreadPoolID, th.MinorFaults, th.MajorFaults,th.UserTime, th.SystemTime,th.TotalTicks, th.IdleTicks, th.SleepTicks, th.BusyTicks, th.TaskRuns, " +
                "th.VoluntaryCtxtSwitches,th.NonVoluntaryCtxtSwitches from master..monThread th, master..monTask tk where th.KTID *= tk.KTID and th.InstanceID *= tk.InstanceID group by th.ThreadID, th.ThreadPoolID,th.InstanceID", threads_end, false);
        } 
        
        
        if (use_monitorTable) {
             if (version_number>=15700){  
                insertQueryToArrayWithoutMetaData(conn, "select BytesSentMB, BytesReceivedMB from  master..monNetworkIO  where InstanceID="+ setInstanceid,  monNetworkIO_end,  false);
                insertQueryToArrayWithoutMetaData(conn, "select LogicalName, sum(IOs), sum(IOTime) from  master..monIOQueue group by LogicalName",  monIOQueue_end,  false);
                insertQueryToArrayWithoutMetaData(conn, "select * from master..monDataCache where InstanceID="+ setInstanceid, monDataCache_end,  false);
                insertQueryToArrayWithoutMetaData(conn, "select * from master..monProcedureCache where InstanceID="+ setInstanceid, monProcedureCache_end,  false);
             }
         }
        
      
        if (isInstanceid) {
             
             Disk_io_structures = getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'udalloc_sleeps'  and InstanceID=" + setInstanceid) - Disk_io_structures ;
             AIOs_delayed_due_to_server_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_server_limit' and InstanceID=" + setInstanceid) - AIOs_delayed_due_to_server_limit;
             AIOs_delayed_due_to_engine_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_engine_limit' and InstanceID=" + setInstanceid) - AIOs_delayed_due_to_engine_limit;
             AIOs_delayed_due_to_os_limit =  getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_os_limit' and InstanceID=" + setInstanceid) - AIOs_delayed_due_to_os_limit;
        
             total_async = getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_dpoll_completed_aios' and InstanceID=" + setInstanceid) - total_async ;
             total_sync =  getLongFromQuery("select  isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_sync_completed_ios' and InstanceID=" + setInstanceid) - total_sync; 
             
             max_outstanding_AIOs_server =  getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'max_outstanding_AIOs_server' and InstanceID=" + setInstanceid) -max_outstanding_AIOs_server ;
      
             insertQueryToArrayWithoutMetaData(conn, "select group_name, field_name, value from master..sysmonitors  where group_name like 'disk_%' and	(field_name = 'total_reads' or field_name = 'total_writes' or field_name ='apf_physical_reads') and InstanceID="+ setInstanceid + " order by field_name, group_name",sysdevices_io_end, true);
             insertQueryToArrayWithoutMetaData(conn, "select group_name, value from master..sysmonitors where field_name = 'max_outstanding_AIOs_engine' and InstanceID="+ setInstanceid, max_outstanding_AIOs_engine_end, true);
         }
        else {
             Disk_io_structures =  getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'udalloc_sleeps' ") - Disk_io_structures ;
             AIOs_delayed_due_to_server_limit =  getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_server_limit' ") - AIOs_delayed_due_to_server_limit;
             AIOs_delayed_due_to_engine_limit =  getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_engine_limit' ") - AIOs_delayed_due_to_engine_limit;
             AIOs_delayed_due_to_os_limit = getLongFromQuery("select isnull(SUM(convert(int, value)), 0)  from master..sysmonitors where group_name like 'engine_%' and field_name = 'AIOs_delayed_due_to_os_limit' ") - AIOs_delayed_due_to_os_limit;
        
             total_async = getLongFromQuery("select isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_dpoll_completed_aios'") - total_async ;
             total_sync = getLongFromQuery("select  isnull(SUM(convert(int, value)), 0) from master..sysmonitors  where field_name = 'total_sync_completed_ios'") - total_sync ; 
            
             max_outstanding_AIOs_server =  getLongFromQuery("select value from master..sysmonitors where group_name = 'kernel' and field_name = 'max_outstanding_AIOs_server'") - max_outstanding_AIOs_server;
       
             insertQueryToArrayWithoutMetaData(conn, "select group_name, field_name, value from master..sysmonitors  where group_name like 'disk_%' and	(field_name = 'total_reads' or field_name = 'total_writes' or field_name ='apf_physical_reads') order by field_name, group_name",sysdevices_io_end, true);
             insertQueryToArrayWithoutMetaData(conn, "select group_name, value from master..sysmonitors where field_name = 'max_outstanding_AIOs_engine'", max_outstanding_AIOs_engine_end, true);
        }
        
   
     
        executeTime = System.currentTimeMillis() - (executeTime + sample_interval_ms);
        logger.debug("Execute time (ms) " + executeTime);
        
        //using sysmonitors_diff
        setColsIds();
        
        logger.debug("sysmonitors_end " + sysmonitors_end.size());
        logger.debug("sysmonitors_begin " + sysmonitors_begin.size());
       
        calculateTime= System.currentTimeMillis();
       
      //  dumpTable(threads_info);
      // System.out.println(max_outstanding_AIOs_engine_end);
       
     
        
        calculateDiff();
             
        setClockTicksAndSeconds();
        
        if (kernelmode.equalsIgnoreCase("threaded")) {
            calculateThreadDiff();
        } //potrzebuje milliseconds 
        
        //setNumXacts();
        
        if (use_monitorTable) {
            calculateCacheDiff();
            calculateIOQueueDiff();
            calculateNetworkIODiff();
        }
        
       
        calculateMax_outstanding_AIOs_engineDiff();        
        calculateLocks();  
        calculateCPUs();
        calculateCPUYields();
        calculateTotalRequestedDiskIOs();
        calculateTotalNetworkIORequests();
        calculateSysdevicesIO();
        
        //System.out.println(getDeviceTotalIOs ("master"));
                  
        
        calculateTime = System.currentTimeMillis() -calculateTime;
        logger.debug("Calculate time (ms) " + calculateTime);
   } 
    
    
   public boolean isStringInMetadataHelper(String compare){
         boolean bl=false;
         for (int i=0; i<metadata_helper.get(0).size(); i++){
            if (metadata_helper.get(0).get(i).equals(compare)) bl=true;
         }
         return bl;
   }
         
    
   public int getNumberOfHits (){
        return hits;
   }
   
    public long getExecuteTime (){
        return executeTime;
     }
    
    public long getCalculateTime (){
        return calculateTime;
    } 
    
    public void clearSysmon(){ 
         attemptClose(conn);
           
        tmpLoad.clear();

        sysmonitors_begin.clear();
        sysmonitors_end.clear();
        sysmonitors_diff.clear();

        engineBusy_cpu_use.clear();
        engineBusy_io_use.clear();
        engineBusy_system_use.clear();
        engineBusy_idle_use.clear();
        engineBusy_user_use.clear();

        CPUYields_cnt.clear();
        CPUYields_cnt_interrupted.clear();    
        
        sp_cluster_show.clear();
        workload_data1.clear();
        workload_data2.clear();
        workload_data3.clear();
        
        sp_monitorconfig.clear();
        sp_lock.clear();
        Locks.clear();
        
        threads_begin.clear();
        threads_end.clear();
        threads_diff.clear();
        //threads_info.clear();
        //engine_info.clear();
        
        monProcedureCache_begin.clear();
        monProcedureCache_end.clear();
        
        monDataCache_begin.clear();
        monDataCache_end.clear();
        
        monIOQueue_begin.clear();
        monIOQueue_end.clear();
        
        monNetworkIO_begin.clear();
        monNetworkIO_end.clear();
        
        max_outstanding_AIOs_engine_begin.clear();
        max_outstanding_AIOs_engine_end.clear();
        
        sysdevices_io_begin.clear();
        sysdevices_io_end.clear();
        
        //engine_maps.clear();
        //threads_pool_maps.clear(); 
        //engine_pool_maps.clear(); 
       
        cols.clear();      
    }
    
    
    private String getDiskForDeviceName (String device_name){
        String ret="default";
        for (int i=0; i<sysdevices.size();i++){
            if (sysdevices.get(i).get(0).equals(device_name)){
                ret=sysdevices.get(i).get(1);
                i=sysdevices.size();
            }
        }
        return ret;
    }
   
    public void destroySysmon(){ 
        clearSysmon();
        
        tmpLoad = null;

        sysmonitors_begin= null;
        sysmonitors_end= null;
        sysmonitors_diff= null;

        engineBusy_cpu_use= null;
        engineBusy_io_use= null;
        engineBusy_system_use= null;
        engineBusy_idle_use= null;
        engineBusy_user_use= null;

        CPUYields_cnt= null;
        CPUYields_cnt_interrupted= null;;    
        
        sp_cluster_show= null;
        workload_data1= null;
        workload_data2= null;
        workload_data3= null;
        
        sp_monitorconfig= null;
        sp_lock= null;
        Locks= null;
        
        threads_begin= null;
        threads_end= null;
        threads_diff= null;
       // threads_info= null;
      //  engine_info= null;
        
        monProcedureCache_begin= null;
        monProcedureCache_end= null;
        
        monDataCache_begin= null;
        monDataCache_end= null;
        
        monIOQueue_begin= null;
        monIOQueue_end= null;
        
        monNetworkIO_begin= null;
        monNetworkIO_end= null;
        
        //engine_maps = null;
        //threads_pool_maps = null; 
       // engine_pool_maps = null; 
      
        cols= null;
     
        logger.info("SysmonEngine completed");
      
    }
      
   /*public void setInstanceIdManualy(int _setInstanceid){
       setInstanceid = _setInstanceid;
   } */
    
   
  
  private int getEngineIds(int engineNo){
      int ids = 0;
      int ret=0;
      boolean wasFound=false;
     
           
      for (int i=0; i<engine_maps.size(); i++){
             
               try {
                   ids = engine_maps.get(i);
               } catch (Exception e){
                 logger.error("MAPOWANIE ENGINOW - " + e.getMessage());
               }
               if (ids == engineNo){
                   ret = i;
                   wasFound = true;
                   break;
               }
             
      }
      if (!wasFound) logger.error("MAPOWANIE ENGINOW - wasFound = false" ); 
      logger.debug("MAPOWANIE ENGINOW - " + engineNo + " -> " + ret);
      return ret;
  }
  
   private int getEngineNo(int ids){
      int engineNo = 0;
                 
      try {
           engineNo = engine_maps.get(ids);
      } catch (Exception e){
          logger.error("MAPOWANIE ENGINOW (IdsToNO) - " + e.getMessage());
      }
      logger.debug("MAPOWANIE ENGINOW (IdsToNO) - " +  ids + " -> " + engineNo);     
      return engineNo;
  }
  
    
  private void setClockTicksAndSeconds(){
      
       try {
           clockTicks = Integer.parseInt(selectColFromArray(selectFromArray(sysmonitors_diff, setInstanceid, "clock_ticks", false, "engine_0", false), col_value_array).get(0));
           timeticks = Integer.parseInt(doQuery("select  @@timeticks"));
       } catch (Exception ignore){
           logger.warn("INIT SYSMON - sp_sysmon '00:00:05', kernel");
            execQuery("sp_sysmon '00:00:05', kernel");
       }
       
        seconds = (int) (clockTicks  * (1.0 * timeticks/1000000));
        miliseconds = (int) (clockTicks  * (1.0 * timeticks/1000));
        
        if (miliseconds < 0) miliseconds  = sample_interval_ms;
        
        logger.debug("miliseconds " + miliseconds + " sample_interval_ms " + sample_interval_ms);
        
   }  
  //do poprawy 
  /*
  private void setNumXacts(){
      
       try {
           ArrayList<String> selectColFromArray = selectColFromArray(selectFromArray(sysmonitors_diff, setInstanceid, "access", false, "xacts", false), col_value_array);
           if (!selectColFromArray.isEmpty())  NumXacts = Integer.parseInt(selectColFromArray.get(0));
           
       } catch (NumberFormatException ignore){}
       
       if (NumXacts==0)  NumXacts = 1;            
   }  */
      
   public int getClockTicks(){
      return clockTicks;
   } 
   
   public String getBufImdbPrivateBufferGrab(){
      
       double ret=0.0;
       
       try {
           ret = Integer.parseInt(selectColFromArray(selectFromArray(sysmonitors_diff, setInstanceid, "buf_imdb_privatebuffer_grab", false, "buffer_0", false), col_value_array).get(0));
       
       } catch (NumberFormatException ignore){}
       
       return returnString(ret);
              
   } 
   
   public int getActiveInstances(){
      return Integer.parseInt(sp_cluster_show.get(0).get(1));
   }     
   
   public int getClusterUsers(){
      return Integer.parseInt(sp_cluster_show.get(0).get(2));
   }   
   
   public String getAverageLoad(){
      return sp_cluster_show.get(0).get(3);
   }  
   
   
   public String getNodeStatus(int instanceId){
          String ret="unknown";
          for (int i=1; i< sp_cluster_show.size(); i++){
                 if (sp_cluster_show.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId) )){
                     ret = sp_cluster_show.get(instanceId).get(2);
                     break;
                 
              }
           }
          return ret;
   } 
   
   
   public int getNodeUsers(int instanceId){
         String ret="unknown";
          for (int i=1; i< sp_cluster_show.size(); i++){
                 if (sp_cluster_show.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId) )){
                     ret = sp_cluster_show.get(instanceId).get(3);
                     break;
                 
              }
           }
          return Integer.parseInt(ret);
   } 
   
   public String getClusterLoadScore(int instanceId){
          String ret="unknown";
          for (int i=1; i< sp_cluster_show.size(); i++){
                 if (sp_cluster_show.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId) )){
                     ret = sp_cluster_show.get(instanceId).get(4);
                     break;
                 
              }
           }
          return ret;
   }  
  
   public String getWorkloadsConnectionRaw(int instanceId){
       String ret="0.0";
       for (int i=0; i<workload_data1.size(); i++){
           if (workload_data1.get(i).size()>=7){
             if (workload_data1.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) ){
                 ret = workload_data1.get(i).get(1);
                 break;
             }
          }
       }
       
       return ret;
   }  
   
   public String getWorkloadsCpuRaw(int instanceId){
       String ret="0.0";
       for (int i=0; i<workload_data1.size(); i++){
           if (workload_data1.get(i).size()>=7){
             if (workload_data1.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) ){
                 ret = workload_data1.get(i).get(2);
                 break;
             }
          }
           
       }
       return ret;
   }  
   
     public String getWorkloadsRunQueueRaw(int instanceId){
       String ret="0.0";
      
       for (int i=0; i<workload_data1.size(); i++){
           if (workload_data1.get(i).size()>=7){
             if (workload_data1.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) ){
                 ret = workload_data1.get(i).get(3);
                 break;
             }
          }
       }
       
       return ret;
   } 
     
   public String getWorkloadsIoLoadRaw(int instanceId){
       String ret="0.0";
       
       for (int i=0; i<workload_data1.size(); i++){
           if (workload_data1.get(i).size()>=7){
             if (workload_data1.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) ){
                 ret = workload_data1.get(i).get(4);
                 break;
             }
          }
       }
       
       return ret;
   } 
   public String getWorkloadsEngineRaw(int instanceId){
       String ret="0.0";
      
       for (int i=0; i<workload_data1.size(); i++){
           if (workload_data1.get(i).size()>=7){
             if (workload_data1.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) ){
                 ret = workload_data1.get(i).get(5);
                 break;
             }
          }
       }
       
       return ret;
   } 
   
   public String getLogicalClusterActiveConnetions(String ClusterName, int instanceId){
       String ret="0.0";
      
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(3);
                 break;
             }
          }
       }
       
       return ret;
   } 

   public String getLogicalClusterState(String ClusterName, int instanceId){
       String ret="0.0";
      
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(4);
                 break;
             }
          }
       }
       
       return ret;
   }  
   
   public String getLogicalClusterLoadScore(String ClusterName, int instanceId){
       String ret="0.0";
       
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(5);
                 break;
             }
          }
       }
       
       return ret;
   }  
   
   public String getLogicalClusterConnectionScore(String ClusterName, int instanceId){
       String ret="0.0";
       
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(6);
                 break;
             }
          }
       }
       
       return ret;
   }
   
    public String getLogicalClusterCPUScore(String ClusterName, int instanceId){
       String ret="0.0";
       
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(7);
                 break;
             }
          }
       }
       
       return ret;
   }  
    
    public String getLogicalClusterRunQueueScore(String ClusterName, int instanceId){
       String ret="0.0";
     
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(8);
                 break;
             }
          }
       }
       
       return ret;
   } 
    
    public String getLogicalClusterIoLoadScore(String ClusterName, int instanceId){
       String ret="0.0";
   
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(9);
                 break;
             }
          }
       }
       
       return ret;
   }  
    
    public String getLogicalClusterEngineScore(String ClusterName, int instanceId){
       String ret="0.0";
       
       for (int i=0; i<workload_data2.size(); i++){
           if (workload_data2.get(i).size()>=9){
             if (workload_data2.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data2.get(i).get(1).equalsIgnoreCase(ClusterName) ){
                 ret = workload_data2.get(i).get(10);
                 break;
             }
          }
       }
       
       return ret;
   }
   
   public String getProfileLoadScore(String profileName, int instanceId){
       String ret="0.0";
      
       for (int i=0; i<workload_data3.size(); i++){
           if (workload_data3.get(i).size()>=9){
             if (workload_data3.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data3.get(i).get(1).equalsIgnoreCase(profileName) ){
                 ret = workload_data3.get(i).get(2);
                 break;
             }
          }
       }
       
       return ret;
   }  
   
    public String getProfileConnectionsScore(String profileName, int instanceId){
       String ret="0.0";
       
       for (int i=0; i<workload_data3.size(); i++){
           if (workload_data3.get(i).size()>=9){
             if (workload_data3.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data3.get(i).get(1).equalsIgnoreCase(profileName) ){
                 ret = workload_data3.get(i).get(3);
                 break;
             }
          }
       }
       
       return ret;
   } 
  
   public String getProfileCPUScore(String profileName, int instanceId){
       String ret="0.0";
     
       for (int i=0; i<workload_data3.size(); i++){
           if (workload_data3.get(i).size()>=9){
             if (workload_data3.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data3.get(i).get(1).equalsIgnoreCase(profileName) ){
                 ret = workload_data3.get(i).get(4);
                 break;
             }
          }
       }
       
       return ret;
   } 
   public String getProfileRunQueueScore(String profileName, int instanceId){
       String ret="0.0";
     
       for (int i=0; i<workload_data3.size(); i++){
           if (workload_data3.get(i).size()>=9){
             if (workload_data3.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data3.get(i).get(1).equalsIgnoreCase(profileName) ){
                 ret = workload_data3.get(i).get(5);
                 break;
             }
          }
       }
       
       return ret;
   } 
   
   public String getProfileIoLoadScore(String profileName, int instanceId){
       String ret="0.0";
      
       for (int i=0; i<workload_data3.size(); i++){
           if (workload_data3.get(i).size()>=9){
             if (workload_data3.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data3.get(i).get(1).equalsIgnoreCase(profileName) ){
                 ret = workload_data3.get(i).get(6);
                 break;
             }
          }
       }
       
       return ret;
   } 
   public String getEngineScore(String profileName, int instanceId){
       String ret="0.0";
     
       for (int i=0; i<workload_data3.size(); i++){
           if (workload_data3.get(i).size()>=9){
             if (workload_data3.get(i).get(0).equalsIgnoreCase(Integer.toString(instanceId)) && workload_data3.get(i).get(1).equalsIgnoreCase(profileName) ){
                 ret = workload_data3.get(i).get(6);
                 break;
             }
          }
       }
       
       return ret;
   } 
   
   
   public String getIOTime(String logicalName){
       double ret=0.0;
      
       for (int i=0; i<monIOQueue_end.size(); i++){
           if (monIOQueue_end.get(i).size()>=3){
             if (monIOQueue_end.get(i).get(0).equalsIgnoreCase(logicalName) ){
                 try {
                     ret = Integer.parseInt(monIOQueue_end.get(i).get(2))/(1.0*seconds);
                 } catch (Exception e){}
                 break;
             }
          }
       }
       
       return returnString(ret);
   }
  
   public String getIOOperations(String logicalName){
       double ret=0.0;
      
       for (int i=0; i<monIOQueue_end.size(); i++){
           if (monIOQueue_end.get(i).size()>=3){
             if (monIOQueue_end.get(i).get(0).equalsIgnoreCase(logicalName) ){
                  try {
                     ret = Integer.parseInt(monIOQueue_end.get(i).get(1))/(1.0*seconds);
                 } catch (Exception e){}
                 break;
             }
          }
       }
       
      return returnString(ret);
   }
   
    public String getNetworkIOBytesSentMB(){
        double ret=0.0;
       
       try {
           ret = Integer.parseInt(monNetworkIO_end.get(0).get(0))/(1.0*seconds);
       } catch (Exception e){} ;   
       
       return returnString(ret);
   }
  
    public String getNetworkIOBytesReceivedMB(){
        double ret=0.0;
       
        try {
            ret = Integer.parseInt(monNetworkIO_end.get(0).get(1))/(1.0*seconds);
        }   catch (Exception e){};  
       
       return returnString(ret);
   }
    
    public String getSp_monitorconfigNumFree(String name){
       return getSp_monitorconfigValue(name, 1);
   }  
    
    public String getSp_monitorconfigNumActive(String name){
       return getSp_monitorconfigValue(name, 2);
   } 
   
   public String getSp_monitorconfigPctAct(String name){
       return getSp_monitorconfigValue(name, 3);
   }
    
   public String getSp_monitorconfigMaxUsed(String name){
       return getSp_monitorconfigValue(name, 4);
   }  
   
   public String getSp_monitorconfigReuseCnt(String name){
       return getSp_monitorconfigValue(name, 5);
   } 
   
   public String getSp_monitorconfigValue(String name, int column){
       String ret="0";
       if (sp_monitorconfig.isEmpty()) ret="-1";
       else {
           for (int i=0; i<sp_monitorconfig.size(); i++){
               if (sp_monitorconfig.get(i).get(0).equalsIgnoreCase(name)){
                     ret = sp_monitorconfig.get(i).get(column);
                     break;
                 }
              }
           }
       
       return ret;
   } 
   
   
   
      
   public String getTotalRequestedDiskIOs(){
      return returnString(TotalRequestedDiskIOs);
   }
   
   public int getNumberOfLocksType(String type){
      if (Locks.get(type)!=null) return Locks.get(type);
      else return 0;
   }
      
   
   public String getTotalNetworkIORequests(){
      return returnString(TotalNetworkIORequests);
   }
 
   public double getAvgRunnableTasks1minEngine(int engine){
     engine = getEngineIds(engine);
       
     isErrorInTest=false;
     if (tmpLoad.size()<engine-1){
          isErrorInTest=true;
          return 0.0;
      } else {
          if (tmpLoad.get(engine).get(0).equals("4") ) {
              double ret=0.0;
              try {
                 //  ret=Double.parseDouble( tmpLoad.get(engine).get(2))/(1.0*NumEngines);  
                   ret=Double.parseDouble( tmpLoad.get(engine).get(2));
              } catch (Exception e){}
              return ret;
         }
         else {
             return 1.0;
         }
      }
   }   
  
   
   public double getAvgRunnableTasks1minAvgEngine(){
       
      isErrorInTest=false;
      double sum=0.0;
      int eng=0;
      
      for (int engine=0; engine<tmpLoad.size(); engine++){
         if (tmpLoad.get(engine).get(0).equals("4") ) {
              try {
                   //sum+=Double.parseDouble( tmpLoad.get(engine).get(2))/(1.0*NumEngines);
                   sum+=Double.parseDouble( tmpLoad.get(engine).get(2));
                   eng++;
              } catch (Exception e){}
         }
      }
      if (eng>0) return sum/(1.0*eng);
      else return 0.0;
      
   }  
   
   public double getAvgRunnableTasks1minGlobalQueue(){
      isErrorInTest=false;
      if (tmpLoad.size()>0){
        if (tmpLoad.get(tmpLoad.size()-1).get(0).equals("5") )  return Double.parseDouble( tmpLoad.get(tmpLoad.size()-1).get(2));
        else return 1.0;
      } else return 0.0;
      
   }  
   
   public double getAvgRunnableTasks5minEngine(int engine){
      engine = getEngineIds(engine);
      isErrorInTest=false;
      if (tmpLoad.size()<engine-1){
          return 0.0;
      } else {
          if (tmpLoad.get(engine).get(0).equals("4") ) {
              double ret=0.0;
              try {
                   //ret=Double.parseDouble( tmpLoad.get(engine).get(3))/(1.0*NumEngines);
                   ret=Double.parseDouble( tmpLoad.get(engine).get(3));
              } catch (Exception e){}
              return ret;
         }
         else return 1.0;
      }
   }    
   
   public double getAvgRunnableTasks5minAvgEngine(){
      isErrorInTest=false;
      double sum=0.0;
      int eng=0;
      
      for (int engine=0; engine<tmpLoad.size(); engine++){
         if (tmpLoad.get(engine).get(0).equals("4") ) {
              try {
                  // sum+=Double.parseDouble( tmpLoad.get(engine).get(3))/(1.0*NumEngines);
                   sum+=Double.parseDouble( tmpLoad.get(engine).get(3));
                   eng++;
              } catch (Exception e){}
         }
      }
      if (eng>0) return sum/(1.0*eng);
      else return 0.0;
      
   }  
   
  public double getAvgRunnableTasks5minGlobalQueue(){
      isErrorInTest=false;
      if (tmpLoad.size()>0){
        if (tmpLoad.get(tmpLoad.size()-1).get(0).equals("5") )  return Double.parseDouble( tmpLoad.get(tmpLoad.size()-1).get(3));
        else return 1.0;
      } else return 0.0;
      
   } 
   
   public double getAvgRunnableTasks15minEngine(int engine){
      engine = getEngineIds(engine);
      isErrorInTest=false;
      if (tmpLoad.size()<engine-1){
          return 0.0;
      } else {
         if (tmpLoad.get(engine).get(0).equals("4") ) {
              double ret=0.0;
              try {
                   //ret=Double.parseDouble( tmpLoad.get(engine).get(4))/(1.0*NumEngines);
                   ret=Double.parseDouble( tmpLoad.get(engine).get(4));
              } catch (Exception e){}
              return ret;
         }
         else return 1.0;
      }
   }    
   
   public double getAvgRunnableTasks15minAvgEngine(){
      isErrorInTest=false;
      double sum=0.0;
      int eng=0;
      
      for (int engine=0; engine<tmpLoad.size(); engine++){
         if (tmpLoad.get(engine).get(0).equals("4") ) {
              try {
                   //sum+=Double.parseDouble( tmpLoad.get(engine).get(4))/(1.0*NumEngines);
                   sum+=Double.parseDouble( tmpLoad.get(engine).get(4));
                   eng++;
              } catch (Exception e){}
         }
      }
      if (eng>0) return sum/(1.0*eng);
      else return 0.0;
      
   }  
   
    public double getAvgRunnableTasks15minGlobalQueue(){
      isErrorInTest=false;
      if (tmpLoad.size()>0){
        if (tmpLoad.get(tmpLoad.size()-1).get(0).equals("5") )  return Double.parseDouble( tmpLoad.get(tmpLoad.size()-1).get(4));
        else return 1.0;
      } else return 0.0;
      
   } 
  
   public double getCPUYieldsFullSleeps_prc(int engine){
     engine = getEngineIds(engine);
     isErrorInTest=false;
     if (CPUYields_cnt.size()<engine-1 || CPUYields_cnt_interrupted.size()<engine-1){
          return 0.0;
      } else if ( (CPUYields_sum+CPUYields_sum_interrupted) == 0.0){
          return 0.0;
      } else {
         return 100.0*CPUYields_cnt.get(engine)/(CPUYields_sum+CPUYields_sum_interrupted); 
      }
   }   
   
    public double getCPUYieldsInterruptedSleeps_prc(int engine){
       engine = getEngineIds(engine);
       isErrorInTest=false;
       if (CPUYields_cnt.size()<engine-1 || CPUYields_cnt_interrupted.size()<engine-1){
          return 0.0;
      } else if ( (CPUYields_sum+CPUYields_sum_interrupted) == 0.0){
          return 0.0;
      } else {
         return 100.0*CPUYields_cnt_interrupted.get(engine)/(CPUYields_sum+CPUYields_sum_interrupted); 
      }
   } 
           
   public double getCPUYields_prc(int engine){
      engine = getEngineIds(engine);
      isErrorInTest=false;
      if (CPUYields_cnt.size()<engine-1){
          return 0.0;
      } else if (CPUYields_sum == 0.0){
          return 0.0;
      } else {
         return 100.0*CPUYields_cnt.get(engine)/CPUYields_sum; 
      }
   } 

  public double getCPUYieldsFullSleeps(int engine){
       return getCPUYields(engine);
  }     

  public double getCPUYieldsInterruptedSleeps(int engine){
      engine = getEngineIds(engine);
      isErrorInTest=false;
      if (CPUYields_cnt_interrupted.size()<engine-1){
          return 0.0;
      } else {
         return CPUYields_cnt_interrupted.get(engine); 
      }
   } 
      
 
  public double getCPUYields(int engine){
      engine = getEngineIds(engine);
      isErrorInTest=false;
      if (CPUYields_cnt.size()<engine-1){
          return 0.0;
      } else {
         return CPUYields_cnt.get(engine); 
      }
  } 
 
  public double getCPUYieldsFullSleeps(){
       return getCPUYields();
  }   
  
  public double getCPUYieldsInterruptedSleeps(){
      return CPUYields_sum_interrupted; 
      
   } 
 
  public double getCPUYields(){
      return CPUYields_sum;
   } 
  
   public long getMax_outstanding_AIOs_engine(int engine){
      long ret=-1; 
      for (int i=0; i<max_outstanding_AIOs_engine_end.size(); i++){
          if (max_outstanding_AIOs_engine_end.get(i).get(0).equals("engine_"+engine)){
              ret = Long.parseLong(max_outstanding_AIOs_engine_end.get(i).get(1));
              break;
          }
      }
      return ret;
   }

  public long getMax_outstanding_AIOs_server(){
      return max_outstanding_AIOs_server;
  }  
  
  public long getDelayedByDiskIOStructures(){
      return Disk_io_structures;
  }
  
  public long getDelayedByServerConfigLimit(){
      return AIOs_delayed_due_to_server_limit;
  }
  
  public long getDelayedByEngineConfigLimit(){
      return AIOs_delayed_due_to_engine_limit;
  }
   
   public long getDelayedByOperatingSystemLimit(){
      return AIOs_delayed_due_to_os_limit;
  }  
   
   public String  getTotalCompletedAsynchronousIOs(){
       double ret=-1.0;
       try {
            ret = total_async/(1.0*seconds);
       }   catch (Exception e){};  
       
      return returnString(ret);  
  }
  
   public String  getTotalCompletedSynchronousIOs(){
       double ret=-1.0;
       try {
            ret = total_sync/(1.0*seconds);
       }   catch (Exception e){};  
       
      return returnString(ret);  
  }  
   
   public String  getTotalCompleteIOs(){
       double ret=-1.0;
       try {
            ret = (total_async+total_sync)/(1.0*seconds);
       }   catch (Exception e){};  
       
      return returnString(ret);  
  } 
   
   public String  getDeviceReadsAPF(String device){
        double ret=0.0;
        String dev_name=getDiskForDeviceName (device);
        for (int i=0; i<sysdevices_io_end.size(); i++){  
            if (sysdevices_io_end.get(i).get(0).equals(dev_name) && sysdevices_io_end.get(i).get(1).equals("apf_physical_reads")){
                ret = Integer.parseInt(sysdevices_io_end.get(i).get(2))/(1.0*seconds);
                i=sysdevices_io_end.size(); //break
            }
        }
        
        return returnString(ret);  
   }   
   
   public String  getDeviceReadsNonAPF(String device){
        double ret=0.0;
        int tmp=0;
        String dev_name=getDiskForDeviceName (device);
        int i=0;
        for (i=0; i<sysdevices_io_end.size(); i++){  
            if (sysdevices_io_end.get(i).get(0).equals(dev_name) && sysdevices_io_end.get(i).get(1).equals("total_reads")){
                tmp = Integer.parseInt(sysdevices_io_end.get(i).get(2));
                i=sysdevices_io_end.size(); //break
            }
        }  
        
        for (i=0; i<sysdevices_io_end.size(); i++){  
            if (sysdevices_io_end.get(i).get(0).equals(dev_name) && sysdevices_io_end.get(i).get(1).equals("apf_physical_reads")){
                ret = (tmp - Integer.parseInt(sysdevices_io_end.get(i).get(2)))/(1.0*seconds);;
            }
        }
        
        
        return returnString(ret);  
   }
   
    public String  getDeviceWrites (String device){
       double ret=0.0;
        String dev_name=getDiskForDeviceName (device);
        for (int i=0; i<sysdevices_io_end.size(); i++){  
            if (sysdevices_io_end.get(i).get(0).equals(dev_name) && sysdevices_io_end.get(i).get(1).equals("total_writes")){
                ret = Integer.parseInt(sysdevices_io_end.get(i).get(2))/(1.0*seconds);
                i=sysdevices_io_end.size(); //break
            }
        }
        
        return returnString(ret);   
   }
  
    public String  getDeviceTotalIOs(String device){
        double ret=0.0;
        int tmp=0;
        String dev_name=getDiskForDeviceName (device);
        int i=0;
        for (i=0; i<sysdevices_io_end.size(); i++){  
            if (sysdevices_io_end.get(i).get(0).equals(dev_name) && sysdevices_io_end.get(i).get(1).equals("total_reads")){
                tmp = Integer.parseInt(sysdevices_io_end.get(i).get(2));
                i=sysdevices_io_end.size(); //break
            }
        }  
        
        for (i=0; i<sysdevices_io_end.size(); i++){  
            if (sysdevices_io_end.get(i).get(0).equals(dev_name) && sysdevices_io_end.get(i).get(1).equals("total_writes")){
                ret = (tmp + Integer.parseInt(sysdevices_io_end.get(i).get(2)))/(1.0*seconds);;
            }
        }
        
        
        return returnString(ret);  
   }
   
   
 
    
  public double getThreadPoolEngineUtilIOBusy(String poolname){
     int engine;
     double ret=0.0;
     int _hits=0;
     for (String l : getEngineNumbersForThreadPoolName(poolname)){
           engine = getEngineIds(Integer.parseInt(l));
           ret = ret + engineBusy_io_use.get(engine);
           _hits++;
          
     }
     if (_hits>0) ret = ret / (1.0*_hits);
     return ret;    
   } 
   
   public double getThreadPoolEngineUtilUserBusy(String poolname){
     int engine;
     double ret=0.0;
     int _hits=0;
     for (String l : getEngineNumbersForThreadPoolName(poolname)){
           engine = getEngineIds(Integer.parseInt(l));
           ret = ret + engineBusy_user_use.get(engine);
           _hits++;
          
     }
     if (_hits>0) ret = ret / (1.0*_hits);
     return ret;    
   }   
   
   public double getThreadPoolEngineUtilSystemBusy(String poolname){
     int engine;
     double ret=0.0;
     int _hits=0;
     for (String l : getEngineNumbersForThreadPoolName(poolname)){
           engine = getEngineIds(Integer.parseInt(l));
           ret = ret + engineBusy_system_use.get(engine);
           _hits++;
          
     }
     if (_hits>0) ret = ret / (1.0*_hits);
     return ret;    
   } 
   public double getThreadPoolEngineUtilIdleBusy(String poolname){
     int engine;
     double ret=0.0;
     int _hits=0;
     for (String l : getEngineNumbersForThreadPoolName(poolname)){
           engine = getEngineIds(Integer.parseInt(l));
           ret = ret + engineBusy_idle_use.get(engine);
           _hits++;
          
     }
     if (_hits>0) ret = ret / (1.0*_hits);
     return ret;    
   } 
      
   public double getEngineUtilIOBusy(int engine){
     engine = getEngineIds(engine);
      if (engineBusy_io_use.size()<engine-1){
          return 0.0;
      } else return engineBusy_io_use.get(engine);
   } 
   
   public double getEngineUtilIOBusy(){      
       double sum=0.0;
       sum = engineBusy_io_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item);       
       return sum/engineBusy_io_use.size();
   }  
   
   public double getEngineUtilUserBusy(int engine){
      engine = getEngineIds(engine);
      if (engineBusy_user_use.size()<engine-1){
          return 0.0;
      } else 
      return engineBusy_user_use.get(engine);
   } 
   
   public double getEngineUtilUserBusy(){
       double sum=0.0;
       sum = engineBusy_user_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item);       
       return sum/engineBusy_user_use.size();
   } 
   
    public double getEngineUtilSystemBusy(int engine){
      engine = getEngineIds(engine);
      if (engineBusy_system_use.size()<engine-1){
          return 0.0;
      } else return engineBusy_system_use.get(engine);
   } 
   
   public double getEngineUtilSystemBusy(){
       double sum=0.0;
       sum = engineBusy_system_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item);       
       return sum/engineBusy_system_use.size();
   }   
   
   public double getEngineUtilUserPlusSystemBusy(int engine){
      engine = getEngineIds(engine);
      if (engineBusy_system_use.size()<engine-1 || engineBusy_user_use.size()<engine-1){
          return 0.0;
      } else return engineBusy_system_use.get(engine) + engineBusy_user_use.get(engine);
   } 
  
   public double getEngineUtilUserPlusSystemBusy(){
       double sum=0.0;
       sum = engineBusy_system_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item); 
       sum = engineBusy_user_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item); 
       return sum/engineBusy_system_use.size();
   } 
   
   
   public double getEngineUtilIdleBusy(int engine){
      engine = getEngineIds(engine);
       if (engineBusy_idle_use.size()<engine-1){
          return 0.0;
      } else return engineBusy_idle_use.get(engine);
   } 
   
    public double getEngineUtilIdleBusy(){
       double sum=0.0;
       sum = engineBusy_idle_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item);       
       return sum/engineBusy_idle_use.size();
   } 
    
   public double getEngineUtilCPUBusy(int engine){
     engine = getEngineIds(engine);
      if (engineBusy_cpu_use.size()<engine-1){
          return 0.0;
      } else 
      return engineBusy_cpu_use.get(engine);
   }   
   
   public double getEngineUtilCPUBusy(){
       double sum=0.0;
       sum = engineBusy_cpu_use.stream().map((d) -> d).reduce(sum, (accumulator, _item) -> accumulator + _item);       
       return sum/engineBusy_cpu_use.size();
   } 
   
   private String getThreadPoolIDForThreadPoolName(String poolname){
       String ret="-1";
       for (int i=0; i<threads_pool_maps.size(); i++){
           if (threads_pool_maps.get(i).get(2).equalsIgnoreCase(poolname)){
             ret = threads_pool_maps.get(i).get(1); 
             break;
           }
       }
       return ret;
   } 
   
   private List<String> getEngineNumbersForThreadPoolName(String poolname){
       String thpoolid = getThreadPoolIDForThreadPoolName(poolname);
           
       List<String> eng = new ArrayList<>();
       for (int i=0; i<engine_pool_maps.size(); i++){
           //System.out.println(engine_pool_maps.get(i).get(1));
           if (engine_pool_maps.get(i).get(1).equals(thpoolid)){
            eng.add(engine_pool_maps.get(i).get(0));
           }
       }
       return eng;
   }
   
   
   public String getThreadPoolThreadUserUtilization(String pool){
      isErrorInTest=false;
      double ret=0.0;
      String id = getThreadPoolIDForThreadPoolName(pool);
      int _hits=0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(2).equalsIgnoreCase(id) ){
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(5)); //user
                  _hits++;
              } catch (Exception e){
                  logger.warn(e.getMessage());
              }
          }
      }
      if (ret < 0 ) {
        logger.error(" getThreadPoolThreadUserUtilization( ret < 0");
        ret=0;
      }
      if (_hits==0) isErrorInTest=true;
      else ret = ret / (1.0*_hits);
      return returnString(ret);

   }
   
   public String getThreadPoolThreadSystemUtilization(String pool){
      isErrorInTest=false;
      double ret=0.0;
      String id = getThreadPoolIDForThreadPoolName(pool);
      int _hits=0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(2).equalsIgnoreCase(id) ){
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(6)); //system
                  _hits++;
              } catch (Exception e){}
          }
      } 
      if (ret < 0 ) {
        logger.error("getThreadPoolThreadSystemUtilization( ret < 0");
        ret=0;
      }
      
      if (_hits==0) isErrorInTest=true;    
      else ret = ret / (1.0*_hits);
      return returnString(ret);

   }
   
    public String getThreadPoolThreadIdleUtilization(String pool){
      isErrorInTest=false;
      double ret=0.0;
      String id = getThreadPoolIDForThreadPoolName(pool);
      int _hits=0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(2).equalsIgnoreCase(id) ){
              try {
                  ret =  ret + (100.0 - (Double.parseDouble(threads_diff.get(m).get(5)) + Double.parseDouble(threads_diff.get(m).get(6)))); //idle
                  _hits++;
              } catch (Exception e){}
          }
      }
      if (ret < 0 ) {
        logger.error("getThreadPoolThreadIdleUtilization ret < 0");
        ret=0;
      }
      
      if (_hits==0) isErrorInTest=true;
      else ret = ret / (1.0*_hits);
      return returnString(ret);

   }
    
     public String getThreadUserUtilizationAvg(){
      isErrorInTest=false;
      double ret=0.0;
      int _hits=0;
  
           
      for (int m=0; m<threads_diff.size(); m++){
              try {
                  //System.out.println(threads_diff.get(m).get(5)); 
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(5)); //user
                  _hits++;
              } catch (Exception e){
                  // e.printStackTrace();
                  logger.warn("getThreadUserUtilizationAvg " + e.getMessage());
              }
          
      }
     
      if (ret < 0 ) {
        logger.error(" getThreadUserUtilizationAvg ret < 0");
        ret=0;
      }
      
      if (_hits==0) isErrorInTest=true;
      else ret = ret / (1.0*_hits);
      return returnString(ret);

   }
   
   public String getThreadSystemUtilizationAvg(){
      isErrorInTest=false;
      double ret=0.0;
      int _hits=0;
      
      for (int m=0; m<threads_diff.size(); m++){
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(6)); //system
                  _hits++;
              } catch (Exception e){}
          
      } 
      if (ret < 0 ) {
        logger.error("getThreadSystemUtilizationAvg ret < 0");
        ret=0;
      }
      
      if (_hits==0) isErrorInTest=true;    
      else ret = ret / (1.0*_hits);
      return returnString(ret);

   }
   
    public String getThreadIdleUtilizationAvg(){
      isErrorInTest=false;
      double ret=0.0;
      int _hits=0;
      
      for (int m=0; m<threads_diff.size(); m++){
              try {
                   ret =  ret + (100.0 - (Double.parseDouble(threads_diff.get(m).get(5)) + Double.parseDouble(threads_diff.get(m).get(6)))); //idle
                  _hits++;
              } catch (Exception e){}
          
      }
      if (ret < 0 ) {
        logger.error("getThreadIdleUtilizationAvg ret < 0");
        ret=0;
      }
      
      if (_hits==0) isErrorInTest=true;
      else ret = ret / (1.0*_hits);
      return returnString(ret);

   }
     
   public String getThreadUserUtilization(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret = Double.parseDouble(threads_diff.get(m).get(5)); //user
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
      return returnString(ret);

   }   
   
   public String getThreadSystemUtilization(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
           if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret = Double.parseDouble(threads_diff.get(m).get(6)); //system
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
      return returnString(ret);

   }   
   
   
   public String getThreadIdle(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret = 100.0 - (Double.parseDouble(threads_diff.get(m).get(5)) + Double.parseDouble(threads_diff.get(m).get(6))); //idle
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
       return returnString(ret);

   } 
   
   public String getMajorFaults(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret =Double.parseDouble(threads_diff.get(m).get(4));
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
       return returnString(ret);

   }  
   
   public String getTotalMajorFaults(){
      isErrorInTest=false;
      double ret=0.0;
      
      for (int m=0; m<threads_diff.size(); m++){
      
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(4));
              } catch (Exception e){}
              break;
          
      }
      return returnString(ret);

   }
   
   public String getMinorFaults(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret =Double.parseDouble(threads_diff.get(m).get(3));
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
       return returnString(ret);

   } 
   
   public String getTotalMinorFaults(){
      isErrorInTest=false;
      double ret=0.0;
      
      for (int m=0; m<threads_diff.size(); m++){
      
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(3));
              } catch (Exception e){}
              break;
          
      }
      return returnString(ret);

   }
   
   public String getTaskRuns(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret =Double.parseDouble(threads_diff.get(m).get(11));
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
       return returnString(ret);

   }
   
     public String getVoluntaryCtxSwitches(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret =Double.parseDouble(threads_diff.get(m).get(12));
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
       return returnString(ret);

   }
     
   public String getTotalVoluntaryCtxSwitches(){
      isErrorInTest=false;
      double ret=0.0;
            
      for (int m=0; m<threads_diff.size(); m++){
      
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(12));  
              } catch (Exception e){}
              break;
          
      }
      return returnString(ret);

   }
     
   public String getNonVoluntaryCtxSwitches(String thread, String engine){
      isErrorInTest=false;
      double ret=-1.0;
      
      for (int m=0; m<threads_diff.size(); m++){
          if (threads_diff.get(m).get(0).equalsIgnoreCase(thread) && threads_diff.get(m).get(1).equalsIgnoreCase(engine)){
              try {
                  ret =Double.parseDouble(threads_diff.get(m).get(13));
              } catch (Exception e){}
              break;
          }
      }
      if (ret<0) isErrorInTest=true;
       return returnString(ret);

   }  
   
   public String getTotalNonVoluntaryCtxSwitches(){
      isErrorInTest=false;
      double ret=0.0;
      
      for (int m=0; m<threads_diff.size(); m++){
      
              try {
                  ret = ret + Double.parseDouble(threads_diff.get(m).get(13));
              } catch (Exception e){}
              break;
          
      }
      return returnString(ret);

   }
   
   public String getProcedureCacheHitRatio(){
      isErrorInTest=false;
      double ret=0.0;
      
      try {
         ret = (Integer.parseInt(monProcedureCache_end.get(0).get(0))-Integer.parseInt(monProcedureCache_end.get(0).get(1)))*100.0/( 1.0*(Integer.parseInt(monProcedureCache_end.get(0).get(0))) );
      } catch (Exception e){}
     
      return returnString(ret);

   }  
   
   public String getDataCacheHitRatio(String cache_name){
      isErrorInTest=false;
      double ret=0.0;
      
      for (int m=0;m <monDataCache_end.size(); m++){
          
                
          if (monDataCache_end.get(m).get(13).equalsIgnoreCase(cache_name) ){
              try {
                ret = 100.0-Integer.parseInt(monDataCache_end.get(0).get(5))*100.0/(1.0*Integer.parseInt(monDataCache_end.get(0).get(4)));
                if (Integer.parseInt(monDataCache_end.get(0).get(5))>Integer.parseInt(monDataCache_end.get(0).get(4))){
                    ret = 0.0;
                }
              } catch (Exception e){}
              break;
          }
      }
      
      try {
       
      } catch (Exception e){}
     
      return returnString(ret);

   }  
   
   public String getDataInMemoryCacheHitRatio(String cache_name){
      isErrorInTest=false;
      double ret=0.0;
      
      for (int m=0;m <monDataCache_end.size(); m++){
      
          if (monDataCache_end.get(m).get(13).equalsIgnoreCase(cache_name) ){
             try {
                ret = Integer.parseInt(monDataCache_end.get(0).get(7))*100.0/(1.0*Integer.parseInt(monDataCache_end.get(0).get(4)));
             } catch (Exception e){}
             break;
          }
      }     
  
     
      return returnString(ret);

   }
   
   
 
   
   public String getThreadCpuUnitsConsuming(){
        isErrorInTest=false;
        return returnString(threadCpuUse);
   } 
     
   public void calculateLocks(){  
     
      try { 
         
        Locks.put("shared_page_row", 0);
        Locks.put("exclusive_page_row", 0);   
        Locks.put("update_page_row", 0);   
        
        Locks.put("shared_table", 0);
        Locks.put("exclusive_table", 0);     
        Locks.put("intent", 0);
        Locks.put("demand", 0);
        
        if (sp_lock.size()>0){
              int kol = sp_lock.get(0).size(); 
              int kk = 3;
              if (kol==10) kk=3; else 
              if (kol==11) kk=4;
              int instanceid=0; 
              boolean _execute=true;
                      
              for (int i=0; i< sp_lock.size();i++){
                  _execute=true;
                  if (kol==11) {
                      try {
                          instanceid=Integer.parseInt(sp_lock.get(i).get(0));
                      } catch (Exception ignore){}
                     if (instanceid!=setInstanceid) _execute=false;
                  } 
                  
                  if (_execute){
                  
                      if (sp_lock.get(i).get(kk).startsWith("Ex_intent") || sp_lock.get(i).get(kk).startsWith("Ex_row") || sp_lock.get(i).get(kk).startsWith("Ex_page")){
                           int nolocks = Locks.get("exclusive_page_row");
                           Locks.put("exclusive_page_row",nolocks+1);
                      } else if (sp_lock.get(i).get(kk).startsWith("Sh_intent") || sp_lock.get(i).get(kk).startsWith("Sh_row") || sp_lock.get(i).get(kk).startsWith("Sh_page")){
                           int nolocks = Locks.get("shared_page_row");
                           Locks.put("shared_page_row",nolocks+1);
                      } else if (sp_lock.get(i).get(kk).startsWith("Update")){
                           int nolocks = Locks.get("update_page_row");
                           Locks.put("update_page_row",nolocks+1);
                      } else if (sp_lock.get(i).get(kk).startsWith("Ex_table")){
                           int nolocks = Locks.get("shared_table");
                           Locks.put("exclusive_table",nolocks+1);
                      } else if (sp_lock.get(i).get(kk).startsWith("Sh_table")){
                           int nolocks = Locks.get("shared_table");
                           Locks.put("shared_table",nolocks+1);
                      } 

                      if (sp_lock.get(i).get(kk).contains("intent")){
                           int nolocks = Locks.get("intent");
                           Locks.put("intent",nolocks+1);
                      } 

                      if (sp_lock.get(i).get(kk).contains("demand")){
                           int nolocks = Locks.get("demand");
                           Locks.put("demand",nolocks+1);
                      }
                  }
             }

              
          }
         
       
        /*for (String key: Locks.keySet()) {
               System.out.println("key : " + key);
               System.out.println("value : " + Locks.get(key));
         }*/
          
     } catch (Exception ignore){}
       
      
   }   
  
    
   public void calculateCPUs(){      
     
               
      for (int v=0; v<NumEngines; v++){
          try { 
             int i =  getEngineNo(v);
             
             int clock_ticks = Integer.parseInt(selectFromArray(sysmonitors_diff, setInstanceid, "clock_ticks", true, "engine_"+i, true).get(0).get(col_value_array) );
    
             if (clock_ticks ==0){
                 logger.error("clock_ticks ==0");
                 clock_ticks = 1;
             }
                  
             if (selectFromArray(sysmonitors_diff, setInstanceid, "idle_ticks", true, "engine_"+i, true).size()>0) {
               //  System.out.println(selectFromArray(sysmonitors_diff, setInstanceid, "idle_ticks", true, "engine_"+i, true).get(0).get(col_value_array));
                 engineBusy_idle_use.add(100.0*Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "idle_ticks", true, "engine_"+i, true).get(0).get(col_value_array) ) /clock_ticks );
             }
             if (selectFromArray(sysmonitors_diff, setInstanceid, "io_ticks", true, "engine_"+i, true).size()>0){
                 engineBusy_io_use.add(100.0*Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "io_ticks", true, "engine_"+i, true).get(0).get(col_value_array) ) /clock_ticks);
             } 
             if (selectFromArray(sysmonitors_diff, setInstanceid, "cpu_ticks", true, "engine_"+i, true).size()>0){
                 engineBusy_cpu_use.add(100.0*Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "cpu_ticks", true, "engine_"+i, true).get(0).get(col_value_array) ) /clock_ticks);
             } 
             if (selectFromArray(sysmonitors_diff, setInstanceid, "system_ticks", true, "engine_"+i, true).size()>0){
                 engineBusy_system_use.add(100.0*Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "system_ticks", true, "engine_"+i, true).get(0).get(col_value_array) ) /clock_ticks);
             } 
             if (selectFromArray(sysmonitors_diff, setInstanceid, "user_ticks", true, "engine_"+i, true).size()>0){
                 engineBusy_user_use.add(100.0*Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "user_ticks", true, "engine_"+i, true).get(0).get(col_value_array) ) /clock_ticks);
             }
         } catch (Exception ignore){
                logger.error("IGNORE " + ignore);         
         }
          
      } 
      
   }    
   
   public void calculateTotalRequestedDiskIOs(){
      isErrorInTest=false;
      try {  
        TotalRequestedDiskIOs = Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "udalloc_calls", true, "kernel", true).get(0).get(col_value_array) ) / (1.0*seconds);
        if (TotalRequestedDiskIOs<0) TotalRequestedDiskIOs=-1*TotalRequestedDiskIOs;
      } catch (NumberFormatException ignore){}
       
      
   }   
 
   public void calculateTotalNetworkIORequests(){
      isErrorInTest=false;
      try {  
         TotalNetworkIORequests = Double.parseDouble(selectFromArray(sysmonitors_diff, setInstanceid, "ksalloc_calls", true, "kernel", true).get(0).get(col_value_array) ) / (1.0*seconds);
         if (TotalNetworkIORequests<0) TotalNetworkIORequests=-1*TotalNetworkIORequests;
      } catch (NumberFormatException ignore){}
       
      
   }  
   
   
   public void calculateCPUYields(){  
     CPUYields_sum = 0.0;
     for (int v=0; v<NumEngines; v++){
         
          int i =  getEngineNo(v);
         
          try { 
           
              
              if (version_number>=15700) { 
                 double tmp_int=Double.parseDouble(selectColFromArray(selectFromArray(sysmonitors_diff, setInstanceid, "engine_sleeps", false, "engine_"+i, true), col_value_array).get(0))/(1.0*seconds);
               
                 double tmp_int2=Double.parseDouble(selectColFromArray(selectFromArray(sysmonitors_diff, setInstanceid, "engine_sleep_interrupted", false, "engine_"+i, true), col_value_array).get(0))/(1.0*seconds);
                 
                 if (tmp_int>tmp_int2) tmp_int=tmp_int-tmp_int2;
                 else tmp_int=0.0; 
                 
                CPUYields_sum += tmp_int; 
                CPUYields_cnt.add(tmp_int);
                 
                 CPUYields_sum_interrupted += tmp_int2; 
                 //System.out.println(i + " -> " + tmp);
                 CPUYields_cnt_interrupted.add(tmp_int2);
              } else {
                double tmp=Double.parseDouble(selectColFromArray(selectFromArray(sysmonitors_diff, setInstanceid, "engine_sleeps", false, "engine_"+i, false), col_value_array).get(0))/(1.0*seconds);
                CPUYields_sum += tmp; 
                CPUYields_cnt.add(tmp);
              }
          } catch (NumberFormatException ignore){}
      } 
      
   }  
   
   private Connection getConnection(String type, int idpool, String host, int port, String user, String password, int timeout) {
           Properties prop = new Properties();

           prop.setProperty("USER", user);
           prop.setProperty("PASSWORD", password);
           prop.setProperty("APPLICATIONNAME", "thread.single.sybase");
          
           if (type.equalsIgnoreCase("single")){

               try {
                   Class.forName("com.sybase.jdbc4.jdbc.SybDriver");
                   DriverManager.setLoginTimeout(timeout);
                   Connection connection =  DriverManager.getConnection("jdbc:sybase:Tds:"+host+":" + port, prop);
                   connection.setCatalog("master");
               if(connection!=null){
                   logger.debug("Connected to " + host+":"+port);
               }
               return connection;
              } catch (ClassNotFoundException e) {
                     logger.error("ClassNotFoundException:");
                     logger.error("error message=" + e.getMessage());
                     return null;

              } catch (SQLException e) {
                  while(e != null) {
                                logger.error("SQL Exception/Error:");
                                logger.error("error message=" + e.getMessage());
                                logger.error("SQL State= " + e.getSQLState());
                                logger.error("Vendor Error Code= " + e.getErrorCode());

                                // it is possible to chain the errors and find the most
                                // detailed errors about the exception
                                e = e.getNextException( );
                   }
                   return null;
              } catch (Exception e2) {
                // handle non-SQL exception …
                  logger.error("SQL Exception/Error:");
                  logger.error("error message=" + e2.getMessage());

                  return null;
              }
           } else {
               logger.debug("POOLING Connected to idpool " +  idpool + " conn " + sybase_host +  ":" + sybase_port);
               Connection connection = DataSource.getInstance(idpool).getConnection();
             
               try { 
                   //BUG 20180208  
                   connection.setCatalog("master");

               } catch (Exception ex) {
                       logger.error(ex);
               }
             
              
               return connection;
           }

     } 
   
   static void attemptClose(Connection o)
    {
	try
	    { if (o != null) o.close();}
	catch (Exception e)
	    {
                logger.error("ERROR 0005" + e);
            }
    }
      
   static void attemptClose(ResultSet o)
    {
	try
	    { if (o != null) o.close();}
	catch (Exception e)
	    {
                logger.error("ERROR 0006" + e);
            }
    }

    static void attemptClose(Statement o)
    {
	try
	    { if (o != null) o.close();}
	catch (Exception e)
	    {
                logger.error("ERROR 0007" + e);
            }
    }  
    
    protected boolean checkForTSQLPrint(SQLException ex)  {
         boolean returnVal = false;
         if ((ex.getErrorCode() == 0) && (ex.getSQLState() == null))
         {
           returnVal = true;

         }
         return returnVal;
     }
        
    protected String printWarnings(SQLException ex){
         StringBuilder retString=new StringBuilder();
         String newline = System.getProperty("line.separator");

         while (ex != null)
         {
           if (checkForTSQLPrint(ex))
           {
               retString.append(ex.getMessage());
               retString.append(newline);
               ex = ex.getNextException();
           } else {
               retString.append(ex.getMessage());
               retString.append(newline);
               ex = ex.getNextException();
           }
         }

         return retString.toString();
             
    }
    
    private void setColsIds(){
         if (sysmonitors_diff.isEmpty()){
             logger.error("sysmonitors_diff.isEmpty()");
             return;
         }
        
         for (int i=0; i<sysmonitors_diff.get(0).size(); i++){
             switch (sysmonitors_diff.get(0).get(i)) {
                 case "field_name":
                     col_field_name_array = i;
                     break;
                 case "group_name":
                     col_group_name_array = i;
                     break;
                 case "value":
                     col_value_array = i;
                     break;               
                 case "instanceid":
                     col_instanceid_array = i;
                     break;
                
                 default:
                     break;
             }
         }
    } 
    
    private void insertQueryToArrayOnlyMetaData(Connection conn, String sql, ArrayList<ArrayList<String>> array, boolean replaceSpace){
        insertQueryToArray(conn, sql,  array, true, false, replaceSpace);
    } 
  
    private void insertQueryToArrayWithMetaData(Connection conn, String sql, ArrayList<ArrayList<String>> array, boolean replaceSpace){
        insertQueryToArray(conn, sql,  array, true, true, replaceSpace);
    } 
    
    private void insertQueryToArrayWithoutMetaData(Connection conn, String sql, ArrayList<ArrayList<String>> array, boolean replaceSpace){
        insertQueryToArray(conn, sql,  array, false, true, replaceSpace);
    }
             
    private void insertQueryToArray(Connection conn, String sql, ArrayList<ArrayList<String>> array, boolean addMetadata, boolean addData, boolean replaceSpace){
    
            if (type.equalsIgnoreCase("pool")){
                conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
            }
        
            if (conn == null){
            }
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
             int numCol=1;
             int updates = 0;
              
             try {  
             
           
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                          
               isResult = stmt.execute(sql);
             
               SQLWarning sqlW = null;  
               sqlW = stmt.getWarnings();
                              
               do {
                 
                  sqlW = stmt.getWarnings();
                                       
                  if (sqlW != null) { 
                        stmt.clearWarnings();
                  } 
                  
                  if (isResult) {
                 
                      rs = stmt.getResultSet();
                      rsmd = rs.getMetaData();
                      numCol = rsmd.getColumnCount();
                      
                      if (!hasMoreResults){

                          //numCol = rsmd.getColumnCount();
                          if (addMetadata) {
                              array.add(new ArrayList<>());
                              for (int i=1; i<=numCol; i++) {
                                 if ( replaceSpace) array.get(0).add(rsmd.getColumnName(i).replaceAll("\\s+",""));
                                 else array.get(0).add(rsmd.getColumnName(i).replaceAll("\\s+"," "));
                              }

                          } 
                      }     
                 
                      String cell="";
                      while (rs.next()) {   
                         if (addData){
                            array.add(new ArrayList<>());
                            for (int i=1; i<=numCol; i++) {
                                 cell = rs.getString(i);
                             
                                 if (cell!=null){
                                    if ( replaceSpace) array.get(array.size()-1).add(cell.replaceAll("\\s+",""));
                                    else array.get(array.size()-1).add(cell.replaceAll("\\s+"," "));
                                 } else {
                                     array.get(array.size()-1).add("null");
                                 }
                             }                                
                                 
                         }
                      }
                              
                       
                    /*int rowsSelected = stmt.getUpdateCount();
                    if (rowsSelected >= 0){
                      
                    }*/
                     
                        
                  }  else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         stmt.clearWarnings();
                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while (hasMoreResults  || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);

            } 
              catch (SQLException e) {  
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
              } finally {
                 if (type.equalsIgnoreCase("pool"))  attemptClose(conn);
             }   
             
             rs = null;
             rsmd = null;
      } 
  
    
    private void insertQueryToIntegerColumn(Connection conn, String sql, ArrayList<Integer> array){
    
            if (type.equalsIgnoreCase("pool")){
                conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
            }
        
            if (conn == null){
            }
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
             int numCol=1;
            
             try {  
             
           
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
               isResult = stmt.execute(sql);
               int updates = 0;
               SQLWarning sqlW = null;  
               sqlW = stmt.getWarnings();
                 
               do {
                   
                  sqlW = stmt.getWarnings();
                                       
                  if (sqlW != null) { 
                        stmt.clearWarnings();
                  } 
                               
                  if (isResult) {
                 
                      rs = stmt.getResultSet();
                      rsmd = rs.getMetaData();
                                           
                      numCol = rsmd.getColumnCount();
                                            
                      int cell=0;
                      while (rs.next()) {
                           
                           cell = rs.getInt(numCol);
                           if (!rs.wasNull()){
                                 array.add(cell);
                           } else {
                                 array.add(0);
                           }
                      }
                      
                  } 
                  
                  hasMoreResults = stmt.getMoreResults();
                  isResult = hasMoreResults;
                }  while (hasMoreResults);
            

                attemptClose(rs);
                attemptClose(stmt);

            } 
              catch (SQLException e) {  
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
              } finally {
                 if (type.equalsIgnoreCase("pool"))  attemptClose(conn);
             }   
             
             rs = null;
             rsmd = null;
     }
    
    private void insertQueryToIntegerArray(Connection conn, String sql, ArrayList<ArrayList<Integer>> array){
    
            if (type.equalsIgnoreCase("pool")){
                conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
            }
        
            if (conn == null){
            }
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
             int numCol=1;
             
             int updates = 0;
          
            
             try {  
             
           
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
               isResult = stmt.execute(sql);
            
               SQLWarning sqlW = null;  
               sqlW = stmt.getWarnings();
                 
               do {
                   
                  sqlW = stmt.getWarnings();
                                       
                  if (sqlW != null) { 
                        stmt.clearWarnings();
                  } 
                               
                  if (isResult) {
                 
                      rs = stmt.getResultSet();
                      rsmd = rs.getMetaData();
                     
                      numCol = rsmd.getColumnCount();
                     
                      int cell=0;
                      while (rs.next()) {
                           array.add(new ArrayList<>());
                           for (int i=1; i<=numCol; i++) {
                                 cell = rs.getInt(i);
                                 if (!rs.wasNull()){
                                     array.get(array.size()-1).add(cell);
                                 } else {
                                     array.get(array.size()-1).add(0);
                                 }
                                 
                                 
                           }
                      }
                      
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         stmt.clearWarnings();
                       }
                   } 
                  
                  hasMoreResults = stmt.getMoreResults();
                  isResult = hasMoreResults;
                }  while (hasMoreResults || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);

            } 
              catch (SQLException e) {  
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
              } finally {
                 if (type.equalsIgnoreCase("pool"))  attemptClose(conn);
             }   
             
             rs = null;
             rsmd = null;
      }
    
    private void clearSample(){
         if (system_view.equalsIgnoreCase("cluster")){
            execQuery("dbcc set_scope_in_cluster('cluster')");
        }
        execQuery("dbcc monitor('clear', 'all', 'on')");
    }
    
    private void getSample(){
        if (system_view.equalsIgnoreCase("cluster")){
            execQuery("dbcc set_scope_in_cluster('cluster')");
        }
        execQuery("dbcc monitor('sample', 'all', 'on')");
       
        
    }
    
    private void calculateDiff(){
             
        if (sysmonitors_begin.size()!=sysmonitors_end.size()){
            logger.error("ERROR x begin,end");
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
        int sysmonitors_diff_size = sysmonitors_diff.size();
        
        for (int i=0; i<sysmonitors_begin.size(); i++){ //i=0 metadata
            sysmonitors_diff.add(new ArrayList<>());
            try {
                v_begin = Long.parseLong(sysmonitors_begin.get(i).get(col_value_array));
            }catch (Exception ignore){} 
            
            try {
                v_end = Long.parseLong(sysmonitors_end.get(i).get(col_value_array));
            }catch (NumberFormatException ignore){}
            
            v_diff = v_end-v_begin;
            
            if (v_diff<0){
                logger.warn("sysmonitors v_diff < 0 " + sysmonitors_end.get(i));
            }
            
            for (int k=0; k<sysmonitors_begin.get(i).size(); k++){
                 if (k!=col_value_array){
                     sysmonitors_diff.get(sysmonitors_diff_size+i).add(sysmonitors_begin.get(i).get(k));
                 } else {
                     sysmonitors_diff.get(sysmonitors_diff_size+i).add(""+v_diff);
                 }
            }
        }
    }
    
    
    private void execQuery(String sql){
          
           if (type.equalsIgnoreCase("pool")){
                conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
            }
         
            if (conn == null){
                return;
            }
            
        
              
             ResultSet rs = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
            
             try {  
             
           
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                          
               isResult = stmt.execute(sql);
          
               int updates = 0;
              // SQLWarning sqlW = null;  
              // sqlW = stmt.getWarnings();
                 
               do {
                    //sqlW = stmt.getWarnings();
                                       
                    //if (sqlW != null) { 
                   //     stmt.clearWarnings();
                    //} 
                 
                  if (isResult) {
                              
                       
                    //int rowsSelected = stmt.getUpdateCount();
                   // if (rowsSelected >= 0){
                          //  out.append(rowsSelected + "  row(s) affected");
                      
                    //}
                     
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                      //sqlW = stmt.getWarnings();
                       //if (sqlW != null)
                      // {
                      //   stmt.clearWarnings();
                      // }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while ((hasMoreResults) || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);
                //attemptClose(conn);
               

            } 
              catch (SQLException e) { 
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
              } finally {
                  if (type.equalsIgnoreCase("pool")){
                    attemptClose(conn);
                 }
             }
      }
    
    
    private Long getLongFromQuery(String sql){
        long ret = 1;
        try {
            ret= Long.parseLong(doQuery(sql));
        } catch (Exception e){
            //e.printStackTrace();
            logger.error(e + " || FOR Q " + doQuery(sql) + " || " + sql);
            ret = -1L;
        }
        return ret;
    }
    
    
   /* private String doFullQuery(String sql){
            
            String ret="default";
             
            StringBuilder out = new StringBuilder();
     
            if (type.equalsIgnoreCase("pool")){
                conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
            }
          
            if (conn == null){
                 return "-20";
            }  
              
             ResultSet rs = null;
             ResultSetMetaData rsmd = null;
             boolean isResult = false;
             boolean hasMoreResults = false;
            
             try {                  
                           
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
              
              
               isResult = stmt.execute(sql);
          
               int updates = 0;
               SQLWarning sqlW = null;  
               sqlW = stmt.getWarnings();
                 
               do {
                    sqlW = stmt.getWarnings();
                                       
                    if (sqlW != null) { 
                        out.append(printWarnings(sqlW));                      
                        stmt.clearWarnings();
                    } 
                 
                  if (isResult) {
                 
                      rs = stmt.getResultSet();
                      rsmd = rs.getMetaData();
                    
                      int numCol = rsmd.getColumnCount();
                      for (int i=1; i<=numCol; i++) {
                            out.append(rsmd.getColumnName(i));
                            out.append(" | ");

                      } 
                               
                      
                                       
                     while (rs.next()) {
                           
                           for (int i=1; i<=numCol; i++) {
                               
                                    String add = rs.getString(i);
                                    if (rs.wasNull()) {
                                        add = "(NULL)";
                                    } else {
                                    }
                                 
                                    
                                
                                 
                           }
                      }
                       
                    
                    int rowsSelected = stmt.getUpdateCount();
                    if (rowsSelected >= 0){
                         out.append(rowsSelected).append("  row(s) affected");                      
                     }
                     
                        
                  } else   {
                      
                       updates = stmt.getUpdateCount();
                       sqlW = stmt.getWarnings();
                       if (sqlW != null)
                       {
                         //logger.warn(sqlW);
                         stmt.clearWarnings();
                       }
                       if ((updates >= 0)) {
                          out.append(updates + " row(s) updated");             
                       }
                   }
                  
                    hasMoreResults = stmt.getMoreResults();
                    isResult = hasMoreResults;
                }  while ((hasMoreResults) || (updates != -1));
            

                attemptClose(rs);
                attemptClose(stmt);

            }  catch (SQLException e) {             
                                   
                    logger.error("SQL Exception/Error for query " + sql);
                    
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
                    
             } finally {
                  executeTime = System.currentTimeMillis()-executeTime;
             }
             
             ret=out.toString();
          
             return ret;
             
    }*/
    
    private String doQuery(String sql){
            if (type.equalsIgnoreCase("pool")){
                conn = getConnection(type, idpool, sybase_host, sybase_port, sybase_user, sybase_password, timeout);
            }
          
            if (conn == null){
                 return "-20";
            }  
            
             String ret="";
             ResultSet rs = null;
                    
             try {  
             
           
               Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                           
               rs = stmt.executeQuery(sql);
               logger.debug(sql);
             
             
                while (rs.next()) {
                      ret = rs.getString(1); 
                       logger.debug("> " + ret);                        
                }
           

                attemptClose(rs);
                attemptClose(stmt); 


            } 
              catch (SQLException e) {
                    logger.error("SQL Exception/Error:");
                    logger.error("error message=" + e.getMessage());
                    logger.error("SQL State= " + e.getSQLState());
                    logger.error("Vendor Error Code= " + e.getErrorCode());
                    logger.error("SQL Exception/Error: ", e);
             } finally {
                  if (type.equalsIgnoreCase("pool")){
                    attemptClose(conn);
                 }
             }
             if (ret==null) ret="null";
             ret=ret.replaceAll("\\s+","");
             return ret;
      }

   
    public ArrayList<ArrayList<String>> selectFromArray(ArrayList<ArrayList<String>> array, int instanceid, String seq_field, boolean eq_field, String seq_group, boolean eq_group){
       
         if (array.isEmpty()){
             return null;
         } 
         ArrayList<ArrayList<String>> select = new ArrayList<>();
         
         int id=0; 
         for (ArrayList<String> a : array){  
              boolean search_ok = false;  
              String field = a.get(col_field_name_array );
              String group = a.get(col_group_name_array );
              if (instanceid>0) {
                  try {
                      String _getInstanceid = a.get(col_instanceid_array);
                      int getInstanceid = -1;
                      if (_getInstanceid.equalsIgnoreCase("instanceid")){
                          getInstanceid=-1;
                      } else {
                          getInstanceid = Integer.parseInt(_getInstanceid);
                      }
                      if (instanceid==getInstanceid) {
                          search_ok=true;
                      }
                  } catch (Exception e){}
             } else {
                  search_ok=true;
             }
              
             if (search_ok){
                  search_ok=false;  //reset
                  if (eq_field){
                      if (field.equals(seq_field)){
                          search_ok = true;
                      }
                  } else {
                      if (field.startsWith(seq_field)){
                          search_ok = true;
                      }
                  }

                  if (search_ok){
                      search_ok=false;  
                      if (eq_group){
                          if (group.equals(seq_group)){
                              search_ok = true;
                          }
                      } else {
                          if (group.startsWith(seq_group)){
                              search_ok = true;
                          }
                      }

                  }
              }
            
              
              if (search_ok) select.add(selectRowFromArray(array, id));
              id++;
              
          }
         
         return select;
     }   
     
     public ArrayList<String> selectRowFromArray(ArrayList<ArrayList<String>> array, int row){
         if (array.size()>row){
             return array.get(row);
         } else {
             return null;
         }
         
     }
     
     public ArrayList<String> selectColFromArray(ArrayList<ArrayList<String>> array, int col){
         if (cols!=null) cols.clear(); 
         if (array.isEmpty()) return cols;
         if (array.get(0).size()<col){
             return null;
         }
         array.forEach((s) -> {
             cols.add(s.get(col));
         });
         
         return cols;
         
     }
     
     
     //******************************* nowe **********************************
    private void calculateThreadDiff(){
             
        if (threads_begin.size()!=threads_end.size()){
            //logger.error("ERROR x begin,end");
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
        
       // dumpTable(threads_begin); 
      //  System.out.println("-------");
       // dumpTable(threads_end);
        boolean found=false;        
        for (int i=0; i<threads_begin.size(); i++){ 
            if ( Integer.parseInt(threads_begin.get(i).get(0))!=setInstanceid){ 
                continue;
            }
                        
            for (int kolumn=1; kolumn<threads_begin.get(i).size(); kolumn++){ //k=0 instance
               if (kolumn==1){
                      found=false; 
                      for (int p=0; p<threads_info.size(); p++){
                       if (threads_info.get(p).get(0).equals(threads_begin.get(i).get(1))){ //ThreadId=id
                            threads_diff.add(new ArrayList<>());
                            threads_diff.get(threads_diff.size()-1).add("Thread " + threads_info.get(p).get(0));
                            threads_diff.get(threads_diff.size()-1).add(threads_info.get(p).get(1));
                           // System.out.println("Thread " + threads_info.get(p).get(0) + " (" + threads_info.get(p).get(1)+")");
                            found=true;
                            break;
                        }
                     }
                     if (!found){
                       for (int p=0; p<engine_info.size(); p++){
                           if (engine_info.get(p).get(0) == Integer.parseInt(threads_begin.get(i).get(1))){
                                threads_diff.add(new ArrayList<>());
                                threads_diff.get(threads_diff.size()-1).add("Thread " + engine_info.get(p).get(0));
                                threads_diff.get(threads_diff.size()-1).add("Engine " + engine_info.get(p).get(1));
                                //System.out.println("Thread " + engine_info.get(p).get(1) + " (Engine " + engine_info.get(p).get(0)+")");
                                found=true;
                                break;
                            }
                         }
                     
                     }
                     
                     if (!found){ //pozostałe
                          threads_diff.add(new ArrayList<>());
                          threads_diff.get(threads_diff.size()-1).add("Thread " + threads_begin.get(i).get(1));
                          threads_diff.get(threads_diff.size()-1).add("PoolID " + threads_begin.get(i).get(2));
                          found=true;
                     }                     
                 }  else if ( kolumn==2 && found){ //ThreadId, PoolID -- kolumna 2 bez robienia roznicy
                     threads_diff.get(threads_diff.size()-1).add(threads_begin.get(i).get(kolumn));
                 }  else if (found) {
                    try {
                        v_begin = Long.parseLong(threads_begin.get(i).get(kolumn));
                    }catch (Exception ignore){
                        logger.error("IGNORE 2 " + ignore);
                    } 

                    try {
                        v_end = Long.parseLong(threads_end.get(i).get(kolumn));
                    }catch (NumberFormatException ignore){
                       logger.error("IGNORE 3 " + ignore);
                    }
                    
                     v_diff = v_end-v_begin;

                    if (v_diff<0){
                       // System.out.println("v_diff < 0" + threads_end.get(i));
                        logger.warn("threads_end v_diff < 0" + threads_end.get(i));
                    }
                    threads_diff.get(threads_diff.size()-1).add(""+v_diff);
                 } else {
                     logger.error("kolumn==2 && found ?????");
                 }
            }
        }
      
        //dumpTable(threads_diff);
       //correct
        threadCpuUse=0.0;
        
        
        int new_miliseconds = (int) (Integer.parseInt(threads_diff.get(1).get(7)) * (1.0 * timeticks/1000));
      
       // System.out.println(new_miliseconds);
        //System.out.println(miliseconds);
        int tmp;
        double tmp1;
        
        for (int k=0; k<threads_diff.size(); k++){ 
                      
            try {
                tmp = Integer.parseInt(threads_diff.get(k).get(5)); //user time
                threadCpuUse = threadCpuUse+tmp;
                tmp1 = 100.0*tmp/(1.0*new_miliseconds);
                threads_diff.get(k).set(5, ""+tmp1);
                              
                tmp = Integer.parseInt(threads_diff.get(k).get(6)); //system time
                threadCpuUse = threadCpuUse+tmp;
                tmp1 = 100.0*tmp/(1.0*new_miliseconds);
                threads_diff.get(k).set(6, ""+tmp1);
                                
                tmp = Integer.parseInt(threads_diff.get(k).get(3)); 
                tmp1 = 1000.0*tmp/(1.0*new_miliseconds);
                threads_diff.get(k).set(3, ""+tmp1);
                
                tmp = Integer.parseInt(threads_diff.get(k).get(4));
                tmp1 = 1000.0*tmp/(1.0*new_miliseconds);
                threads_diff.get(k).set(4, ""+tmp1);  
                
                tmp = Integer.parseInt(threads_diff.get(k).get(12)); 
                tmp1 = 1000.0*tmp/(1.0*new_miliseconds);
                threads_diff.get(k).set(12, ""+tmp1);
                
                tmp = Integer.parseInt(threads_diff.get(k).get(13));
                tmp1 = 1000.0*tmp/(1.0*new_miliseconds);
                threads_diff.get(k).set(13, ""+tmp1);
             
                 
             } catch (Exception e){e.printStackTrace();}
        }
       // dumpTable(threads_diff);
        threadCpuUse = threadCpuUse/(1.0*new_miliseconds);
    }
    
     private void calculateCacheDiff(){
             
        if ( monProcedureCache_begin.size()!=monProcedureCache_end.size()){
            //logger.error("ERROR x begin,end");
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
        boolean err=false;
          
        for (int i=0; i<monProcedureCache_begin.size(); i++){         
            for (int k=0; k<monProcedureCache_begin.get(i).size(); k++){ //k=0 instance
            
                    try {
                        v_begin = Long.parseLong(monProcedureCache_begin.get(i).get(k));
                    }catch (Exception ignore){} 

                    try {
                        v_end = Long.parseLong(monProcedureCache_end.get(i).get(k));
                    }catch (NumberFormatException ignore){}
                    
                     v_diff = v_end-v_begin;

                    if (v_diff<0){
                       // System.out.println("v_diff < 0" + threads_end.get(i));
                        logger.warn("monProcedureCache v_diff < 0" + monProcedureCache_end.get(i));
                    }
                 
                    if (!err) monProcedureCache_end.get(i).set(k, ""+v_diff);
                    else monProcedureCache_end.get(i).set(k, monProcedureCache_begin.get(i).get(k));
                    
            }
            
        }
     
        for (int i=0; i<monDataCache_begin.size(); i++){     
            err=false;
            for (int k=0; k<monDataCache_begin.get(i).size(); k++){ //k=0 instance
            
                    try {
                        v_begin = Long.parseLong(monDataCache_begin.get(i).get(k));
                    }catch (Exception ignore){
                        err=true;
                    } 
                    if (!err){
                        try {
                            v_end = Long.parseLong(monDataCache_end.get(i).get(k));
                        }catch (NumberFormatException ignore){}

                        v_diff = v_end-v_begin;
                    }

                    if (v_diff<0 && !err){
                       // System.out.println("v_diff < 0" + threads_end.get(i));
                        logger.warn("monDataCache v_diff < 0" + monDataCache_end.get(i));
                    }
                    if (!err) monDataCache_end.get(i).set(k, ""+v_diff);
                    else monDataCache_end.get(i).set(k, monDataCache_begin.get(i).get(k));
                 }
            
        }
       
      //  dumpTable(monProcedureCache_diff);
      //  dumpTable(monDataCache_diff);
    
      
    }
     
    private void calculateIOQueueDiff(){
             
        if ( monIOQueue_begin.size()!=monIOQueue_end.size()){
            logger.error("ERROR IOQueue begin,end");
            logger.error("ERROR IOQueue begin " + monIOQueue_begin.size());
            logger.error("ERROR IOQueue end " + monIOQueue_end.size());
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
          
           
        for (int i=0; i<monIOQueue_begin.size(); i++){           
            for (int k=0; k<monIOQueue_begin.get(i).size(); k++){ //k=0 
                
                    if (k>0) {
                         try {
                            v_begin = Long.parseLong(monIOQueue_begin.get(i).get(k));
                        }catch (Exception ignore){} 

                        try {
                            v_end = Long.parseLong(monIOQueue_end.get(i).get(k));
                        }catch (NumberFormatException ignore){}

                         v_diff = v_end-v_begin;

                        if (v_diff<0){
                           // System.out.println("v_diff < 0" + threads_end.get(i));
                            logger.warn("monIOQueue v_diff < 0" + monIOQueue_end.get(i));
                        }
                       monIOQueue_end.get(i).set(k, ""+v_diff);
                    }
                }
            
        }
       
      
    }
    
    private void calculateMax_outstanding_AIOs_engineDiff(){
             
        if ( max_outstanding_AIOs_engine_begin.size()!=max_outstanding_AIOs_engine_end.size()){
            logger.error("ERROR max_outstanding_AIOs_engine_begin begin,end");
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
          
           
        for (int i=0; i<max_outstanding_AIOs_engine_begin.size(); i++){          
        
                
                    try {
                        v_begin = Long.parseLong(max_outstanding_AIOs_engine_begin.get(i).get(1));
                    }catch (Exception ignore){} 

                    try {
                        v_end = Long.parseLong(max_outstanding_AIOs_engine_end.get(i).get(1));
                    }catch (NumberFormatException ignore){}

                     v_diff = v_end-v_begin;

                    if (v_diff<0){
                       // System.out.println("v_diff < 0" + threads_end.get(i));
                        logger.warn("max_outstanding_AIOs_engine_ v_diff < 0 " + v_begin + " | " +  v_end);
                    }
                    //                         
                   // max_outstanding_AIOs_engine_diff.get(max_outstanding_AIOs_engine_diff.size()-1).add(""+v_diff); 
                    max_outstanding_AIOs_engine_end.get(i).set(1, ""+v_diff);                    
                   // get(i).set(0, max_outstanding_AIOs_engine_end.get(i).get(0).replaceAll("\\s+", ""));
            
        }
       
      
    }
    
    private void calculateSysdevicesIO(){
             
        if ( sysdevices_io_begin.size()!=sysdevices_io_end.size()){
            logger.error("ERROR sysdevices_io begin,end");
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
          
           
        for (int i=0; i<sysdevices_io_begin.size(); i++){          
         
                
                    try {
                        v_begin = Long.parseLong(sysdevices_io_begin.get(i).get(2));
                    }catch (Exception ignore){} 

                    try {
                        v_end = Long.parseLong(sysdevices_io_end.get(i).get(2));
                    }catch (NumberFormatException ignore){}

                     v_diff = v_end-v_begin;

                    if (v_diff<0){
                       // System.out.println("v_diff < 0" + threads_end.get(i));
                        logger.warn("monNetworkIO v_diff < 0" + sysdevices_io_end.get(i));
                    }
                    sysdevices_io_end.get(i).set(2, ""+v_diff);
                    
                }
            
        
       
      
    }
    
     private void calculateNetworkIODiff(){
             
        if ( monNetworkIO_begin.size()!=monNetworkIO_end.size()){
            logger.error("ERROR monNetworkIO begin,end");
            return;
        }
        
        long v_begin=0, v_end=0, v_diff=0;
          
           
        for (int i=0; i<monNetworkIO_begin.size(); i++){          
            for (int k=0; k<monNetworkIO_begin.get(i).size(); k++){ //k=0 
                
                    try {
                        v_begin = Long.parseLong(monNetworkIO_begin.get(i).get(k));
                    }catch (Exception ignore){} 

                    try {
                        v_end = Long.parseLong(monNetworkIO_end.get(i).get(k));
                    }catch (NumberFormatException ignore){}

                     v_diff = v_end-v_begin;

                    if (v_diff<0){
                       // System.out.println("v_diff < 0" + threads_end.get(i));
                        logger.warn("monNetworkIO v_diff < 0" + monNetworkIO_end.get(i));
                    }
                    monNetworkIO_end.get(i).set(k, ""+v_diff);
                    
                }
            
        }
       
      
    }
     
     
         
    public void dumpTable(ArrayList<ArrayList<String>> array ){
          for (ArrayList<String> a : array){
              
              for (String data : a){
                  System.out.print(data + " |  ");
              }
              System.out.println("");
          }
     }   
     
    public void dumpList(ArrayList<String> array ){
          for (String a : array){
              System.out.print(a + "\n");
          }
     }
}
