
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HandleHDFSData {
	
	private FileSystem fs = null;
	private Configuration cfg = null;
	FSDataOutputStream hdfsOutStream = null;
	
	public HandleHDFSData(String uri) throws IOException{
		cfg = new Configuration();
		cfg.setBoolean("dfs.support.append", true);
		cfg.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
		cfg.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
		fs = FileSystem.get(URI.create(uri), cfg);
	}
	
	public void openHdfsFile(String path) throws IllegalArgumentException, IOException {
		if(fs.exists(new Path(path))){
			hdfsOutStream = fs.append(new Path(path));
		}
		else {
			hdfsOutStream = fs.create(new Path(path));
		}
	}
	
	public Long[] writeHDFSFile(String data) throws IllegalArgumentException, IOException {
		long posStart = hdfsOutStream.getPos();
		hdfsOutStream.write(data.getBytes());
		long posEnd = hdfsOutStream.getPos();
		hdfsOutStream.flush();
		return new Long[] {posStart,posEnd-posStart};
	}
	
	public void writeHDFSFileForIndex(Map<String,String> tmpIndex) throws IOException {
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(hdfsOutStream));
		for(Map.Entry<String, String> entry:tmpIndex.entrySet()) {
			writer.write(entry.getKey()+":"+entry.getValue());
			writer.newLine();
		}
		writer.close();
	}
	
	public void close() throws IOException {
		hdfsOutStream.close();
	}
	
	public String readHDFSFile(String path,long posStart,long length) throws IllegalArgumentException, IOException {
		FSDataInputStream hdfsInStream = fs.open(new Path(path));
		byte[] ioBuffer = new byte[new Long(length).intValue()];
		hdfsInStream.seek(posStart);
		int readLen = hdfsInStream.read(ioBuffer);
		hdfsInStream.close();
		if(readLen!=new Long(length).intValue()) {
			return null;
		}
		else {
			return new String(ioBuffer);
		}
	}
	
	public void readHDFSFileByIndex(String path, Map<String,String> indexing) throws IllegalArgumentException, IOException {
		if(fs.exists(new Path(path))){
			FSDataInputStream hdfsInStream = fs.open(new Path(path));
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsInStream));
			String line = null;
			String[] parts = null;
			while((line=bufferedReader.readLine())!=null) {
				parts = line.split(":");
				if(parts.length==2) {
					indexing.put(parts[0], parts[1]);
				}
			}
			bufferedReader.close();
			hdfsInStream.close();
		}
	}
}
