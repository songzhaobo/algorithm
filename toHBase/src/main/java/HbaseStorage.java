import dao.HBaseDao;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HbaseStorage implements FileStorage {
    //列属性配置
    private int MaxVersions;//最大版本数
    private int MinVersions;//最小版本数
    int Blocksize;//块大小
    boolean BlockCacheEnabled;//块是否可缓存
    private boolean InMemory;//
    private int TimeToLive;//存储时长
    private BloomType BloomFilterType;//存储HFile过滤器
    private Compression.Algorithm CompressionType;//压缩类型
    private boolean CacheBloomsOnWrite;//
    private boolean CacheDataOnWrite;//
    private boolean CacheIndexesOnWrite;//
    private String CompressTags;//
    private int Scope;//
    private boolean KeepDeletedCells;//是否保存删除的数据
    private DataBlockEncoding DataBlockEncoding;//

    private String FamilyName;//列族名
    String tableName;//表名
    private String RegionNum;//分区数

    private String Quorum;//zookeeper主机
    private String Port;
    private String Master;
    private String Parent;

    //    private String Hadoop_Security;
//    private String Hbase_Security;
//    private String Hbase_Master_Kerberos_Principal;
//    private String Hbase_Regionserver_Kerberos_Principal;
//    private String User;
//    private String Path;
//    private String Krb5Path;
    private Configuration configuration = HBaseConfiguration.create();
    private Connection connection;
    private Admin admin;
    private String HBASEENCODE = "utf8";// 编码格式

    //    加载配置文件  获取HBase连接（1）
    public HbaseStorage(Properties properties) {
        this.Port = properties.getProperty("hbase.zookeeper.Port", "2181");
        this.Master = properties.getProperty("hbase.zookeeper.Master");
        this.Quorum = properties.getProperty("hbase.zookeeper.Quorum");
        this.Parent = properties.getProperty("hbase.zookeeper.Parent", "/hbase");
        try {
            connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    获取hbase的连接（2）
    private void connect() throws IOException {
        configuration.set("hbase.zookeeper.property.clientPort", this.Port);
        configuration.set("hbase.zookeeper.quorum", this.Quorum);
        configuration.set("hbase.master", this.Master);
        configuration.set("zookeeper.znode.parent", this.Parent);
        configuration.setInt("hbase.rpc.timeout", 20000);
        configuration.setInt("hbase.client.operation.timeout", 30000);
        configuration.setInt("hbase.client.scanner.timeout.period", 200000);
        configuration.set("hbase.defaults.for.version.skip", "true");
        configuration.set("hbase.client.keyvalue.maxsize", "524288000");
//        获取连接
        connection = ConnectionFactory.createConnection(configuration);
//        从连接中获取表管理对象
        admin = connection.getAdmin();
    }

    //（3）
    public void set(String tablename, String rowkey, String family, HbaseObject hbaseObject) {
        List<String> meta_list = new ArrayList();//存储元数据
        List<byte[]> data_list = new ArrayList();//存储元数据对应的值
        for (String key : hbaseObject.keySet()) {
            meta_list.add(key);
            data_list.add(hbaseObject.getValues(key));
        }
        setToHbase(tablename, rowkey, family, meta_list, data_list);
    }

    //（4）
    private void setToHbase(String tablename, String rowkey, String family, List<String> meta_list, List<byte[]> data_list) {
        String meta = meta_list.get(0);
        byte[] data = data_list.get(0);
        try {
            addData_One(tablename, rowkey, family, meta, data);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

//    入HBase库（5）

    /**
     * 增加一行的记录
     *
     * @param table_name   表名称
     * @param rowkey       行主键
     * @param columnFamily 列族名称
     * @param column       列名(可以为null)
     * @param value        值  该数组的长度=columnFamily的长度*column的长度
     *                     数组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column 如有三个列族，每个列族有2个列  【2,2,2】
     * @throws IOException,NoSuchAlgorithmException
     */
    private void addData_One(String table_name, String rowkey, String columnFamily, String column, byte[] value) throws IOException, NoSuchAlgorithmException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(rowKey(rowkey).getBytes(this.HBASEENCODE));// 指定行
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value);
        table.put(put);
        table.close();
    }

    @Override
//    获取存储系统的名字
    public String name() {
        return "Hbase";
    }

    @Override
//    获取文件完整限定ID
    public String qualifiedId(String id) {
        return null;
    }

    @Override
//判断文件是否存在
    public boolean exist(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
//    获取文件对象
    public FileObject get(String id) {
        return null;
    }

    /**
     * 查询单个row的记录
     *
     * @param table_name   表名
     * @param rowkey       行键
     * @param columnfamily 列族，可以为null
     * @param column       列名，可以为null
     * @return
     * @throws IOException
     */
    public byte[] get(String table_name, String rowkey, String columnfamily, String column) throws IOException, NoSuchAlgorithmException {
        // table_name和row不能为空
        if (StringUtils.isEmpty(table_name) || StringUtils.isEmpty(rowKey(rowkey))) {
            return null;
        }
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(rowKey(rowkey).getBytes(this.HBASEENCODE));
        // 判断在查询记录时,是否限定列族和列名
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addColumn(columnfamily.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)) {
            get.addFamily(columnfamily.getBytes(this.HBASEENCODE));
        }
        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        byte[] bytes = null;
        assert cells != null;
        for (Cell cell : cells) {
            bytes = cell.getValueArray();
        }
        if (bytes != null) {
//            return new ByteArrayInputStream(bytes);
            return bytes;
        } else {
            return null;
        }

    }

    private static String getMD5(String rowkey) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(rowkey.getBytes("UTF8"));
        byte s[] = m.digest();
        String result = "";
        for (int i = 0; i < s.length; i++) {
            result += Integer.toHexString((0x000000ff & s[i]) | 0xffffff00).substring(6);
        }
        return result;
    }


    private String rowKey(String rowkey) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        String head = getMD5(rowkey);
        String row = head.substring(0, 2);
        return row + rowkey;
    }

    /**
     * 获得HBase里面所有的表名
     *
     * @return List Hbase中的表集合
     * @throws IOException
     */
    public List<String> getAllTables() throws IOException {
        List<String> tables = new ArrayList<String>();
        if (admin != null) {
            HTableDescriptor[] allTable = admin.listTables();
            if (allTable.length > 0) {
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                }
            }
        }
        return tables;
    }

    /**
     * 删除一条记录
     *
     * @param table_name 表名
     * @param rowkey     行主键
     * @throws IOException,NoSuchAlgorithmException
     */

    public void deleteRow(String table_name, String rowkey) throws IOException, NoSuchAlgorithmException {
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Delete del = new Delete(rowKey(rowkey).getBytes(this.HBASEENCODE));
        table.delete(del);
    }


    /**
     * 删除表
     *
     * @param table_name 表名
     * @throws IOException
     */
    public void deleteTable(String table_name) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    public List<String> listFile(String tableName, String familyName) {
        ResultScanner scanner = null;
        try {
            scanner = scan_part_Table(tableName, familyName, null, null, null);
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        List<String> fileList = new ArrayList<>();
        for (Result result : scanner) {
            String file = Bytes.toString(result.getRow());
            fileList.add(file);
        }
        return fileList;
    }


    /**
     * 获取表中的部分记录,可以指定列族,列族成员,开始行键,结束行键.
     *
     * @param table_name 表名
     * @param family     列簇
     * @param column     列名
     * @param startRow   开始行主键
     * @param stopRow    结束行主键,结果不包含这行，到它前面一行结束
     * @return
     */
    public ResultScanner scan_part_Table(String table_name, String family, String column, String startRow, String stopRow) throws IOException, NoSuchAlgorithmException {
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
            scan.addColumn(family.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(family) && StringUtils.isEmpty(column)) {
            scan.addFamily(family.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(startRow)) {
            scan.setStartRow(rowKey(startRow).getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(stopRow)) {
            scan.setStopRow(rowKey(stopRow).getBytes(this.HBASEENCODE));
        }
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }

    public void close() {
        try {
            if (admin != null) admin.close();
            if (connection != null) connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    从hbase中下载文件并以流的形式写入到HBASE
    public static void load(List<String> rows) throws IOException {
        HBaseDao hbaseDao = new HBaseDao();
        for (int i = 0; i < rows.size(); i++) {
//       从Hbase中下载文件并将下载下来的byte[],转成字节流
            String rowKey = rows.get(i).substring(2);
//            System.out.println(rowKey);
            InputStream inputStream = hbaseDao.downLoadFile("table99", rowKey, "family", "file");
//            将读取到的流写入到跳板机上的HBASE中，   inputStream.available() 返回流中的字节总数
            hbaseDao.writeHbaseFile(inputStream, rows.get(i), inputStream.available());
        }
    }
//    根据单一的一个rowKey进行读写HBASE
    public static void load(String rowKey) throws IOException {
        HBaseDao hbaseDao = new HBaseDao();
        InputStream inputStream = hbaseDao.downLoadFile("table99", rowKey, "family", "file");
//            将读取到的流写入到跳板机上的HBASE中，   inputStream.available() 返回流中的字节总数
        hbaseDao.writeHbaseFile(inputStream, rowKey, inputStream.available());
    }

    public static void main(String[] args) throws IOException, ConfigurationException {
//       获取所有的rowkey
        HBaseDao hbaseDao = new HBaseDao();
//       获取table99表中的所有rowkey
        List<String> rows = hbaseDao.listFile("table99", "family");
//        System.out.println(rows);
        load(rows);
    }
}
