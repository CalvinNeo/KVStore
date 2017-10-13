//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KVProcessor implements Processor {
    private static final Logger LOG = LogManager.getLogger();
    Map<String, Map<String, String>> store = new HashMap();

    public KVProcessor() {
        LOG.info("kvpod count is " + KvStoreConfig.getServersNum());
    }

    public Map<String, String> get(String key) {
        Map<String, String> table = (Map)this.store.get(key);
        LOG.info("try to find " + key );
        if (table == null){
            LOG.info("can't find " + key + "in " + this.store.size() + " items");
        }else{
        }
        return table;
    }

    public boolean put(String key, Map<String, String> value) {
        LOG.info("Put: current kvpod count is " + KvStoreConfig.getServersNum());
        this.store.put(key, value);
        return true;
    }

    public Map<String, Map<String, String>> filter(Map<String, String> filter) {
        Map<String, Map<String, String>> res = new HashMap();
        Iterator var3 = this.store.keySet().iterator();

        while(var3.hasNext()) {
            String key = (String)var3.next();
            if (this.test(filter, (Map)this.store.get(key))) {
                res.put(key, this.store.get(key));
            }
        }

        return res;
    }

    public int count(Map<String, String> filter) {
        Map<String, Map<String, String>> res = this.filter(filter);
        return res.size();
    }

    public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
        Map var2 = this.store;
        synchronized(this.store) {
            this.store.putAll(records);
            return true;
        }
    }

    public byte[] process(byte[] inupt) {
        System.out.println("receive info:" + new String(inupt));
        return "received!".getBytes();
    }

    public boolean test(Map<String, String> filter, Map<String, String> record) {
        Iterator var3 = filter.keySet().iterator();

        String key;
        do {
            if (!var3.hasNext()) {
                return true;
            }

            key = (String)var3.next();
        } while(((String)filter.get(key)).equals(record.get(key)));

        return false;
    }
}
