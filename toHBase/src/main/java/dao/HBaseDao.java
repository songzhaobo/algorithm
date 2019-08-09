package dao;

import model.FileObject;
import org.apache.commons.codec.digest.DigestUtils;
import storage.HbaseStorge;

import java.io.*;
import java.util.List;
import java.util.Properties;


public class HBaseDao {
    private String row;
    private FileObject fileModel = new FileObject();
    private HbaseStorge storge;
    private String TableName;
    private String Family;

    public HBaseDao(){
        storge = new HbaseStorge();
    }

    private void setToModel(String K ,Byte[] bytes){
        fileModel.set(K, bytes);
    }

    private void save(String tablename ,String family){
        storge.saveToStorge(tablename,this.row,family,fileModel);
    }

    static Byte[] toObjects(byte[] bytesPrim) {
        Byte[] bytes = new Byte[bytesPrim.length];

        int i = 0;
        for (byte b : bytesPrim) bytes[i++] = b; // Autoboxing

        return bytes;
    }

    private static Byte[] getBytes(File file){
        Byte[] buffer = null;
        try {
            FileInputStream fis = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
            byte[] b = new byte[1000];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer =toObjects(bos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    //    往Hbase中写数据
    public String  writeHbaseFile(File file) throws IOException {
        readAddProperties();
        if (!storge.tableExist(this.TableName)){ storge.createTable(); }
        this.row = getMid(file);
        Byte[] bytes = getBytes(file);
        setToModel("file",bytes);
        save(this.TableName,this.Family);
        return this.row;
    }


    //以流的形式存入HBase
    public String writeHbaseFile(InputStream inputStream,String rowkey,int size) throws IOException {
        readAddProperties();
        if (!storge.tableExist(this.TableName)){ storge.createTable(); }
        this.row = rowkey;
        byte[] bytes = new byte[size];
        inputStream.read(bytes);
        setToModel("file",toObjects(bytes));
        save(this.TableName,this.Family);
        inputStream.close();
        return this.row;
    }

    /**
     * 获取插入数据的hbase的配置文件  配置文件信息包括HBASE的表名和列簇名
     * */
    private void readAddProperties()  {
        Properties properties = new Properties();
        try {
            properties.load(HBaseDao.class.getClassLoader().getResourceAsStream("add.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.Family=  properties.getProperty("hbase.family","family");
        this.TableName = properties.getProperty("hbase.table","table");
    }
    /**
     *
     *对文件进行rowkey的获取
     *
     * */
    private String getMid(File file) throws IOException {
        String mid =  getMid(new FileInputStream(file));
        if(mid==null){
            mid = DigestUtils.md5Hex(new FileInputStream(file));
        }
        return mid;
    }

    public String getMid(InputStream inputStream) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
        String mid = null;
        String str = "";
        while ((str = bf.readLine()) != null) {
            if (str.startsWith("Message-ID")) {
                String id[] = str.split(":");
                if (id.length>1){
                    mid = str.split(":")[1].trim();
                    break;
                }
            }
        }

        if (mid != null) {
            mid = mid.replaceAll("^<*", "").replaceAll(">*$", "");
            if (mid.isEmpty()) return null;
        }

        return mid;
    }

    public String getMid1(InputStream inputStream) throws IOException {
        return DigestUtils.md5Hex(inputStream).replaceAll("<","").replaceAll(">","") ;
    }

    public String getTableName(){
        return this.TableName;
    }

    public String getFamily(){
        return this.Family;
    }

    public String getRow(){
        return this.row;
    }

    public void close(){
        storge.close();
    }


    /**
     * 从Hbase中下载文件并将下载下来的byte[],转成字节流
     * */
    public InputStream downLoadFile(String tableName,String rowkey,String family,String column){
        return new ByteArrayInputStream(downLoadFile1(tableName, rowkey, family, column));
    }

    public byte[] downLoadFile1(String tableName,String rowkey,String family,String column){
        return storge.downLoadFromHbase1(tableName, rowkey, family, column);
    }

    //    判断表是否存在
    public boolean exists(String tableName){
        boolean flag = false;
        flag = storge.tableExist(tableName);
        return flag;
    }

    //    获取所有的rowkey
    public List<String> listFile(String tableName,String familyName){
        return storge.listFile(tableName, familyName);
    }
}

