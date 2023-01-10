package com.tencent.hdfs;

import com.tencent.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * TBDS HDFS demo 案例
 */
public class HDFSDemo {
    static Configuration conf = null;
    static HashMap<String,String> params = null;
    static {
      conf = init(false,null);
    }

    public static void main(String[] args) {
        // 添加kerberors认证参数
       /* params = new HashMap<String, String>();
        params.put("hadoop.security.authentication",args[0]);
        params.put("hadoop_security_authentication_tbds_username",args[1]);
        params.put("hadoop_security_authentication_tbds_secureid",args[2]);
        params.put("hadoop_security_authentication_tbds_securekey",args[3]);*/

        try {
            //getAllFile("/");
            //readFile("/myhdfs/syncdata.txt");
            //createFile("/myhdfs/newfile.txt");
            //createDir("/myhdfs/newdir");
            //uploadFile("/tmp/testlog/hsmx.20221018.tmp","/myhdfs/");
            //fileRename("/myhdfs/hsmx.20221018.tmp","/myhdfs/20220109.tmp");
            //deleteFile("/myhdfs/newfile.txt");
            //isFileExists("/myhdfs/20220109.tmp");
            //readFile("/myhdfs/20220109.tmp");
            //fileLastModify("/myhdfs/20220109.tmp");
            //fileLocation("/myhdfs/20220109.tmp");
            getAllFile("/myhdfs/");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param kerberosON 是否在配置文件中已经配置
     * @param params 是否配置其他参数
     * @return
     */
    static Configuration init(boolean kerberosON, HashMap<String,String> params){
        try{
            conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            if(params != null){
                Iterator<Map.Entry<String, String>> iterator = params.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, String> next = iterator.next();
                    conf.set(next.getKey(),next.getValue());
                }
            }
            // 开启认证参数
            if(kerberosON) {
                //加入tbds的认证参数
                conf.set("hadoop.security.authentication", ConfigurationManager.getProperty("hadoop.security.authentication"));
                conf.set("hadoop_security_authentication_tbds_username",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_username"));
                conf.set("hadoop_security_authentication_tbds_secureid",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_secureid"));
                conf.set("hadoop_security_authentication_tbds_securekey",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_securekey"));
            }
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromSubject(null);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conf;
    }

    static FileSystem getFileSystem() throws Exception {
        return FileSystem.get(conf);
    }
    //创建目录
    public static void createDir(String Path) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path dfs = new Path(Path);
        hdfs.mkdirs(dfs);
    }
    //创建文件
    public static void createFile(String FilePath) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path dfs = new Path(FilePath);
        hdfs.create(dfs);
    }
    //上传文件
    public static void uploadFile(String srcPath,String dstPath) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);
        FileStatus files[] = hdfs.listStatus(dst);
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
        System.out.println("--------------------------------");
        hdfs.copyFromLocalFile(src, dst);
        files = hdfs.listStatus(dst);
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }
    //文件重命名
    public static void fileRename(String fileName,String newFileName) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path frpaht = new Path(fileName);
        Path topath = new Path(newFileName);
        boolean isRename = hdfs.rename(frpaht, topath);
        String result = isRename ? "成功" : "失败";
        System.out.println("文件重命名结果：" + result);
    }
    //删除文件或文件内容
    public static void deleteFile(String deletePath) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path delef = new Path(deletePath);
        boolean isDeleted = hdfs.delete(delef, false);
        // 递归删除
        // boolean isDeleted=hdfs.delete(delef,true);
        System.out.println("Delete?" + isDeleted);
    }
    //读取文件
    public static void readFile(String path) throws Exception {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream openStream = fileSystem.open(new Path(path));
        IOUtils.copyBytes(openStream, System.out, 1024, false);
        IOUtils.closeStream(openStream);
    }
    //判断文件是否存在
    public static void isFileExists(String path) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path findf = new Path(path);
        boolean isExists = hdfs.exists(findf);
        System.out.println("Exist?" + isExists);
    }
    //获取文件的最后修改时间
    public static void fileLastModify(String filePath) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path fpath = new Path(filePath);
        FileStatus fileStatus = hdfs.getFileStatus(fpath);
        long modiTime = fileStatus.getModificationTime();
        System.out.println("testcreate的修改时间是" + modiTime);
    }

    //获取文件的存储位置
    public static void fileLocation(String filePath) throws Exception {
        FileSystem hdfs = getFileSystem();
        Path fpath = new Path(filePath);
        FileStatus filestatus = hdfs.getFileStatus(fpath);
        BlockLocation[] blkLocations = hdfs.getFileBlockLocations(filestatus,
                0, filestatus.getLen());
        int blockLen = blkLocations.length;
        for (int i = 0; i < blockLen; i++) {
            String[] hosts = blkLocations[i].getHosts();
            System.out.println("block_" + i + "_location:" + hosts[0]);
        }
    }
    static void getAllFile(String path) throws Exception {
        Path basePath = new Path(path);
          FileStatus[] listStatus = getFileSystem().listStatus(basePath);
    		for (FileStatus fileStatus : listStatus) {
    			System.out.println(fileStatus.getPath()+"------》》》"+fileStatus.toString());
    		}
    }
}
