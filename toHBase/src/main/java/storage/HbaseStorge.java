package storage;

import model.FileObject;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


/**
 * Hbase的存储平台
 */
public class HbaseStorge  {

    static OpHbase opHbase;

   public HbaseStorge() {
        opHbase = new OpHbase();
    }

    /**
     * @param tablename Hbase中的表名
     * @param fileObject     接收数据模型并根据数据模型判断使用RDB模型处理还是File模型处理
     */
    public void saveToStorge(String tablename, String rowkey,String family,FileObject fileObject) {
        fileDataSaveToHbase(tablename,rowkey ,family, fileObject);
    }

    public void close(){
        opHbase.close();
    }


    /**
     * 对文件类模型进行解析
     *
     * @param tablename 需要在Hbase的哪张表下
     * @param fileFileObject 接收File类文件的模型
     */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection","unchecked"})
    private void fileDataSaveToHbase(String tablename, String rowkey , String family, FileObject fileFileObject) {
        List  meta_list = new ArrayList();//存储元数据
        List<Byte[]>  data_list = new ArrayList();//存储元数据对应的值  value[]
        for (String key : fileFileObject.keySet()) {
            meta_list.add(key);  //file
            data_list.add(fileFileObject.getValues(key)); //value[]
        }
        setToHbase(tablename,rowkey,family,meta_list,data_list);

    }
    private void setToHbase(String tablename ,String rowkey, String family,List meta_list , List<Byte[]> data_list)  {
        String[] meta= (String[]) meta_list.toArray(new String[meta_list.size()]);
        Byte[] data = data_list.get(0);
        try {
            opHbase.addData_One(tablename,rowkey,family,meta[0],data);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public boolean tableExist(String tablename)  {
        try {
            return opHbase.tableExist(tablename);
        } catch (IOException e) {
            return false;
        }
    }

    public void createTable(){
        opHbase.createTable();
    }


    public byte[] downLoadFromHbase1(String tablename, String rowkey, String family, String column){
        byte[] bytes = new byte[0];
        try {
            bytes = opHbase.getRow1(tablename, rowkey, family, column);
        } catch (IOException | NoSuchAlgorithmException e) {
            return null;
        }
        if (bytes != null){
            return bytes;
        }else {
            return null;
        }
    }

    public List<String> listFile(String tableName,String familyName){
        ResultScanner scanner = null;
        try {
           scanner= opHbase.scan_part_Table(tableName,familyName,null,null,null);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        List<String> fileList= new ArrayList<>();
        for (Result result:scanner){
            String file = Bytes.toString(result.getRow());
            fileList.add(file);
        }
        return fileList;
    }

}


