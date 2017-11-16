
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.helium.kvstore.common.KvStoreConfig;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcServer;

public class MyProcessor implements Processor{

	private String HDFS_url = null;//hdfs的入口
	private String datapath = null;//hdfs中存放具体数据
	private String indexpath = null;//hdfs中存放数据索引

	private Map<String,String> indexing = null;
	private Map<String,String> tmpIndex = null;
	
	private Map<String,Map<String,String>> store = null;
	private Map<String,Map<String,String>> tmpStore = null;
	
	private HandleHDFSData handler;
	
	private Logger logger = Logger.getLogger(MyProcessor.class);
	
	private String[] datapathList = null;
	private String[] indexpathList = null;
	private Map<String,String>[] indexingList = null;
	
	private int[] errorTime = new int[10];
	
	@SuppressWarnings("unchecked")
	public MyProcessor() {
		HDFS_url = KvStoreConfig.getHdfsUrl();
		datapathList = new String[] {"/datawarehouse1","/datawarehouse2","/datawarehouse3"};
		indexpathList = new String[] {"/index1","/index2","/index3"};
		indexingList = new HashMap[3];
		for(int i=0;i<3;i++) {
			indexingList[i] = new HashMap<String,String>();
		}

		int id = RpcServer.getRpcServerId();
		logger.info("RpcServer.getRpcServerId()"+id);
		datapath = datapathList[id];
		indexpath = indexpathList[id];
		indexing = indexingList[id];
		
		tmpIndex = new HashMap<String,String>();
		store = new HashMap<String,Map<String,String>>();
		tmpStore = new HashMap<String,Map<String,String>>();
		try {
			handler = new HandleHDFSData(HDFS_url);
		} catch (IOException e) {
			logger.error(e);
		}
		loadIndexFromHDFS();
		new Thread(new WriteDataToHDFSThread()).start();
	}

	public boolean batchPut(Map<String, Map<String, String>> arg0) {
		synchronized (tmpStore) {
			tmpStore.putAll(arg0);
		}
		return true;
	}

	public Map<String, String> get(String key) {
		Map<String, String> table = this.store.get(key);

        if (table != null) {
            return table;
        } else {
    		return getValueFromHDFS(key);
        }
	}

	public byte[] process(byte[] input) {

        String mykey = new String(input);
        Map<String, String> replymap = this.store.get(mykey);

        if (replymap == null)
            return null;
        else {

            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(byteOut);
                out.writeObject(replymap);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byte[] replybyte = byteOut.toByteArray();
            return replybyte;
        }
	}

	public boolean put(String arg0, Map<String, String> arg1) {
		synchronized (tmpStore) {
			tmpStore.put(arg0, arg1);
		}
		return true;
	}

	public Map<Map<String, String>, Integer> groupBy(List<String> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public int count(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	private class WriteDataToHDFSThread implements Runnable{
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void run() {
			while(true) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e1) {
				}
				if(datapath==null) {
//					int id = RpcServer.getRpcServerId();
//					System.out.println(id);
					datapath = datapathList[2];
				}
				if(indexpath==null) {
					int id = RpcServer.getRpcServerId();
					indexpath = indexpathList[id];
				}
				if(indexing==null) {
					int id = RpcServer.getRpcServerId();
					indexing = indexingList[id];
				}
				Map<String,Map<String,String>> tmp;
				synchronized (tmpStore) {
					if(tmpStore.keySet().size()==0) {
						continue;
					}
					tmp = (HashMap)((HashMap)tmpStore).clone();
					tmpStore.clear();
				}
				store.putAll(tmp);
				try {
					handler.openHdfsFile(datapath);
					StringBuffer buffer = new StringBuffer();
					for(Map.Entry<String, Map<String,String>> entry:tmp.entrySet()) {
						buffer.append("{").append(entry.toString()).append("}");
						Long info[] = handler.writeHDFSFile(buffer.toString());
						tmpIndex.put(entry.getKey(), String.valueOf(info[0])+"|"+String.valueOf(info[1]));
						buffer.setLength(0);
					}
					handler.close();
					handler.openHdfsFile(indexpath);
					handler.writeHDFSFileForIndex(tmpIndex);
					handler.close();
					indexing.putAll(tmpIndex);
					tmpIndex.clear();
					logger.info("success :"+tmp.size());
				} catch (IllegalArgumentException e) {
					logger.error(e);
					break;
				} catch (IOException e) {
					logger.error(e);
					break;
				}
			}
		}
		
	}
	
	public void loadIndexFromHDFS() {
		try {
			for(int i=0;i<indexpathList.length;i++) {
				handler.readHDFSFileByIndex(indexpathList[i],indexingList[i]);
				logger.info("load index"+i+" successfully!");
			}
		} catch (IllegalArgumentException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		}
	}
	
	public void loadDataFromHDFS() {
		synchronized (store) {
			
		}
	}
	
	public Map<String,String> getValueFromHDFS(String arg0){
		String[] pos = null;
		String searchPath = null;
		for(int i=0;i<indexingList.length;i++) {
			if(indexingList[i].containsKey(arg0)) {
				pos = indexingList[i].get(arg0).split("[|]");
				searchPath = datapathList[i];
				break;
			}
		}
		if(pos!=null) {
			String result = null;
			try {
				result = handler.readHDFSFile(searchPath, Long.valueOf(pos[0]), Long.valueOf(pos[1]));//KvStoreConfig.getHdfsUrl()
			} catch (IllegalArgumentException e) {
				if(errorTime[0]<1) {
					logger.error(e);
				}
				errorTime[0]++;
				return null;
			} catch (IOException e) {
				if(errorTime[1]<1) {
					logger.error(e);
				}
				errorTime[1]++;
				return null;
			}
			if(result!=null) {
				//add log
				if(errorTime[2]<1) {
					logger.info("fetch key-value : "+result);
				}
				Gson gson = new Gson();
				Map<String,Map<String,String>> t = gson.fromJson(result, new TypeToken<Map<String,Map<String,String>>>(){}.getType());
				return t.get(arg0);
			}
			else {
				return null;
			}
		}
		else {
			return null;
		}
	}

}
