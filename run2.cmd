java -classpath ./out/artifacts/kvstore_jar/kvstore.jar cn.helium.kvstore.main.Main --restful-server-port 8501 --kvpod-id 1 --processor-class KVProcessor --hdfs-port 9000 --hdfs-host "127.0.0.1" --kvpods-hosts "localhost,localhost,localhost" --kvpods-rpc-ports "8090,8091,8092"