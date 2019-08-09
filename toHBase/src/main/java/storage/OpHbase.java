package storage;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//HBase操作类
public class OpHbase {
    //列属性配置
    private int MaxVersions;//最大版本数
    private int MinVersions;//最小版本数
    int Blocksize;//块大小
    boolean BlockCacheEnabled;//块是否可缓存
    private boolean InMemory;//是否入内存
    private int TimeToLive;//存储时长
    private BloomType BloomFilterType;//存储HFile过滤器
    private Compression.Algorithm CompressionType;//压缩类型
    private boolean CacheBloomsOnWrite;//
    private boolean CacheDataOnWrite;//写入时缓存数据
    private boolean CacheIndexesOnWrite;//写入时缓存索引
    private String CompressTags;//压缩标签
    private int Scope;//域
    private boolean KeepDeletedCells;//是否保存删除的数据

    private DataBlockEncoding DataBlockEncoding;//数据块编码

    //表属性配置
    private String FamilyName;//列族名
    String tableName;//表名
    private String RegionNum;//分区数

    private String Quorum;//zookeeper主机
    private String Port;
    private String Master;
    private String Parent;
    private String Hadoop_Security;
    private String Hbase_Security;
    private String Hbase_Master_Kerberos_Principal;
    private String Hbase_Regionserver_Kerberos_Principal;
    private String User;
    private String Path;
    private String Krb5Path;
    private Configuration configuration = HBaseConfiguration.create();
    private Connection connection;
    private Admin admin;
    private String HBASEENCODE = "utf8";// 编码格式


    public Configuration getConf() {
        return configuration;
    }

    /**
     * 初始化hbase操作类时传入hbase的配置文件，完成初始化操作 operateHBase（1）
     *
     * @param
     * @param
     * @throws IOException
     */
    public OpHbase() {
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化  （2）
     *
     * @param
     */
    private void init() throws IOException {
        readConf();
        configuration.set("hbase.zookeeper.property.clientPort", this.Port);
//        hbase使用zookeeper的地址
        configuration.set("hbase.zookeeper.quorum", "10.170.130.105,10.170.130.106,10.170.130.107,10.170.130.108,10.170.130.109");
        configuration.set("hbase.master","10.170.130.105");
        configuration.set("zookeeper.znode.parent", this.Parent);
        configuration.setInt("hbase.rpc.timeout", 1800000);
        configuration.setInt("hbase.client.operation.timeout", 30000);
        configuration.setInt("hbase.client.scanner.timeout.period", 1800000);
        configuration.set("hbase.defaults.for.version.skip","true");
        configuration.set("hbase.client.keyvalue.maxsize","524288000");
//        //设置安全验证方式为kerberos
//        System.setProperty("java.security.krb5.conf",this.Krb5Path);
//        configuration.set("hadoop.security.authentication", this.Hadoop_Security);
//        configuration.set("hbase.security.authentication", this.Hbase_Security);
//        //设置hbase master及hbase regionserver的安全标识，这两个值可以在hbase-site.xml中找到
//        configuration.set("hbase.master.kerberos.principal", this.Hbase_Master_Kerberos_Principal);
//        configuration.set("hbase.regionserver.kerberos.principal", this.Hbase_Regionserver_Kerberos_Principal);
//        //使用设置的用户登陆 第一个为KDC注册的用户名字，第二个为KDC生成的注册的用户的密钥文件路径
//        UserGroupInformation.setConfiguration(configuration);
//        UserGroupInformation.loginUserFromKeytab(this.User, this.Path);
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

//（3）

    private void readConf() {
        Properties xmlConfiguration = new Properties();
        try {
//            加载读取HBASE的配置文件
            ClassLoader classLoader = OpHbase.class.getClassLoader();
            xmlConfiguration.load(classLoader.getResourceAsStream("conf.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.Port = xmlConfiguration.getProperty("zookeeper.Port", "2181");
        this.Master = xmlConfiguration.getProperty("zookeeper.Master");
        this.Quorum = xmlConfiguration.getProperty("zookeeper.Quorum");
        this.Parent = xmlConfiguration.getProperty("zookeeper.Parent");
//        this.Hadoop_Security = xmlConfiguration.getProperty("kerberos.HadoopSecurityAuthentication");
//        this.Hbase_Security = xmlConfiguration.getProperty("kerberos.HbaseSecurityAuthentication");
//        this.Hbase_Master_Kerberos_Principal = xmlConfiguration.getProperty("kerberos.HbaseMasterKerberosPrincipal");
//        this.Hbase_Regionserver_Kerberos_Principal = xmlConfiguration.getProperty("kerberos.HbaseRegionserverKerberosPrincipal");
//        this.User = xmlConfiguration.getProperty("kerberos.User");
//        this.Path = xmlConfiguration.getProperty("kerberos.Path");
//        this.Krb5Path = xmlConfiguration.getProperty("kerberos.Krb5Path");
    }

    /**
     * 创建命名空间
     *
     * @param namespacme 命名空间的名字
     * @throws IOException
     */

    public void createNamespace(String namespacme) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespacme).build();
        admin.createNamespace(namespaceDescriptor);
    }

    /*读取建表的配置信息*/
    private void readCreateProperties() {
        Properties xmlConfiguration = new Properties();
        try {
            xmlConfiguration.load(OpHbase.class.getClassLoader().getResourceAsStream("createtable.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.MaxVersions = Integer.parseInt(xmlConfiguration.getProperty("column.MaxVersions"));
        this.MinVersions = Integer.parseInt(xmlConfiguration.getProperty("column.MinVersions"));
        this.Blocksize = Integer.parseInt(xmlConfiguration.getProperty("column.BlockSize"));
        this.BlockCacheEnabled = Boolean.parseBoolean(xmlConfiguration.getProperty("column.BlockCacheEnabled"));
        this.InMemory = Boolean.parseBoolean(xmlConfiguration.getProperty("column.InMemory"));
        this.TimeToLive = Integer.parseInt(xmlConfiguration.getProperty("column.TimeToLive"));
        this.BloomFilterType = BloomType.valueOf(xmlConfiguration.getProperty("column.BloomFilterType").toUpperCase());
        //this.CompressionType = Algorithm.valueOf(xmlConfiguration.getString("column.CompressionType").toUpperCase());
        this.CacheBloomsOnWrite = Boolean.parseBoolean(xmlConfiguration.getProperty("column.CacheBloomsOnWrite"));
        this.CacheDataOnWrite = Boolean.parseBoolean(xmlConfiguration.getProperty("column.CacheDataOnWrite"));
        this.CacheIndexesOnWrite = Boolean.parseBoolean(xmlConfiguration.getProperty("column.CacheIndexesOnWrite"));
        this.CompressTags = xmlConfiguration.getProperty("column.CompressTags");
        this.Scope = Integer.parseInt(xmlConfiguration.getProperty("column.Scope"));
        this.KeepDeletedCells = Boolean.parseBoolean(xmlConfiguration.getProperty("column.KeepDeletedCells"));
        this.DataBlockEncoding = DataBlockEncoding.valueOf(xmlConfiguration.getProperty("column.DataBlockEncoding").toUpperCase());
        this.FamilyName = xmlConfiguration.getProperty("table.FamilyName");
        this.tableName = xmlConfiguration.getProperty("table.TableName");
        this.RegionNum = xmlConfiguration.getProperty("table.RegionNum");
    }

    /**
     * 创建表
     *
     * @param
     * @param
     */

    public void createTable() {
        readCreateProperties();
        String[] family_names = this.FamilyName.split(",");
        try {
            TableName table = TableName.valueOf(this.tableName);
            if (admin.tableExists(table)) {// 如果存在要创建的表，那么先删除，再创建
                System.out.println(table + " has existed");
                return;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(table);
            for (String family_name : family_names) {
                // column 描述
                HColumnDescriptor family = new HColumnDescriptor(family_name);
                family.setBlockCacheEnabled(this.BlockCacheEnabled);
                family.setBlocksize(this.Blocksize);
                family.setBloomFilterType(this.BloomFilterType);
                family.setCacheBloomsOnWrite(this.CacheBloomsOnWrite);
                family.setCacheDataOnWrite(this.CacheDataOnWrite);
                family.setCacheIndexesOnWrite(this.CacheIndexesOnWrite);
                //family.setCompressionType(this.CompressionType);
                family.setDataBlockEncoding(this.DataBlockEncoding);
                family.setInMemory(this.InMemory);
                family.setMaxVersions(this.MaxVersions);
                family.setMinVersions(this.MinVersions);
                family.setTimeToLive(this.TimeToLive);
                family.setKeepDeletedCells(this.KeepDeletedCells);
                family.setScope(this.Scope);
                tableDescriptor.addFamily(family);
            }
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建带有命名空间的表
     *
     * @param namespacename 命名空间
     * @param family_names  列族集合
     */
    public void createTable(String namespacename, String... family_names) {

        try {
            TableName table = TableName.valueOf(namespacename, this.tableName);
            if (admin.tableExists(table)) {// 如果存在要创建的表，返回
                System.out.println(table + " is exist");
                return;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(table);
            for (String family_name : family_names) {
                // column 描述
                HColumnDescriptor family = new HColumnDescriptor(family_name);
                family.setBlockCacheEnabled(this.BlockCacheEnabled);
                family.setBlocksize(this.Blocksize);
                family.setBloomFilterType(this.BloomFilterType);
                family.setCacheBloomsOnWrite(this.CacheBloomsOnWrite);
                family.setCacheDataOnWrite(this.CacheDataOnWrite);
                family.setCacheIndexesOnWrite(this.CacheIndexesOnWrite);
                family.setCompressionType(this.CompressionType);
                family.setDataBlockEncoding(this.DataBlockEncoding);
                family.setInMemory(this.InMemory);
                family.setMaxVersions(this.MaxVersions);
                family.setMinVersions(this.MinVersions);
                family.setTimeToLive(this.TimeToLive);
                family.setKeepDeletedCells(this.KeepDeletedCells);
                family.setScope(this.Scope);
                tableDescriptor.addFamily(family);
            }
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 增加一行的记录
     *
     * @param table_name   表名称
     * @param rowkey       行主键
     * @param columnFamily 列族名称
     * @param column       列名(可以为null)
     * @param value        值  该数组的长度=columnFamily的长度*column的长度
     * @param num          组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column 如有三个列族，每个列族有2个列  【2,2,2】
     * @throws IOException,NoSuchAlgorithmException
     */
    public void addData_One(String table_name, String rowkey, String[] columnFamily, String[] column, String[] value, int[] num) throws IOException, NoSuchAlgorithmException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(Bytes.toBytes(rowKey(rowkey)));// 指定行
//        Put put = new Put(rowKey(rowkey).getBytes(this.HBASEENCODE));// 指定行
        int count = 0;
        for (int i = 0; i < columnFamily.length; i++) {
            for (int j = count; j < count + num[i]; j++) {
                put.addColumn(Bytes.toBytes(columnFamily[i]), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
            }
            count = count + num[i];
        }

        table.put(put);
        table.close();
    }

    public void addData_One(String table_name, String rowkey, String columnFamily, String column, Byte[] value) throws IOException, NoSuchAlgorithmException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(Bytes.toBytes(rowKey(rowkey)));// 指定行
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), toPrimitives(value));
        table.put(put);
        table.close();
    }

    //    将对象转换成原始值
    private byte[] toPrimitives(Byte[] oBytes) {
        byte[] bytes = new byte[oBytes.length];
        for (int i = 0; i < oBytes.length; i++) {
            bytes[i] = oBytes[i];
        }

        return bytes;
    }

    /**
     * 批量插入
     *
     * @param table_name   表名
     * @param rowkeys      行主键
     * @param columnFamily 列簇，表中共有几个列簇
     * @param column       列名，表中共有几个列
     * @param value        值，这个数组的长度=columnFamily的长度*column的长度，顺序是按照行从左到右依次填入
     * @param num          数组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column 如有三个列族，每个列族有2个列  【2,2,2】
     * @throws IOException,NoSuchAlgorithmException
     */
    public void addData_Multi(String table_name, String[] rowkeys, String[] columnFamily, String[] column, String[] value, int[] num) throws IOException, NoSuchAlgorithmException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        ArrayList<Put> list = new ArrayList<Put>();
        int count2 = 0;// 记录value的下标
        for (int m = 0; m < rowkeys.length; m++) {
            Put put = new Put(rowKey(rowkeys[m]).getBytes(this.HBASEENCODE));// 指定行
            int count1 = 0;// 定位column的位置
            for (int i = 0; i < columnFamily.length; i++) {
                for (int j = count1; j < count1 + num[i]; j++) {
                    put.addColumn(columnFamily[i].getBytes(this.HBASEENCODE), column[j].getBytes(this.HBASEENCODE), value[count2].getBytes(this.HBASEENCODE));
                    count2++;
                }
                count1 = count1 + num[i];
            }
            list.add(put);
        }
        table.put(list);
        table.close();
    }


    /**
     * 判断表是否存在
     *
     * @param table_name 表名
     * @return boolen 表是否存在
     * @throws IOException
     */
    public boolean tableExist(String table_name) throws IOException {
        return admin.tableExists(TableName.valueOf(table_name));
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
     * 删除多条记录
     *
     * @param table_name 表名
     * @param rowkeys    需要删除的行主键
     * @throws IOException,NoSuchAlgorithmException
     */
    public void delMultiRows(String table_name, String[] rowkeys) throws IOException, NoSuchAlgorithmException {
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        ArrayList<Delete> delList = new ArrayList<Delete>();
        for (String row : rowkeys) {
            Delete del = new Delete(rowKey(row).getBytes(this.HBASEENCODE));
            delList.add(del);
        }
        table.delete(delList);
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
    public Cell[] getRow(String table_name, String rowkey, String columnfamily, String column) throws IOException, NoSuchAlgorithmException {

        // table_name和row不能为空
        if (StringUtils.isEmpty(table_name) || StringUtils.isEmpty(rowKey(rowkey))) {
            return null;
        }
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
//        Get get = new Get(rowKey(rowkey).getBytes(this.HBASEENCODE));
//        // 判断在查询记录时,是否限定列族和列名
//        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
//            get.addColumn(columnfamily.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
//        }
//        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)) {
//            get.addFamily(columnfamily.getBytes(this.HBASEENCODE));
//        }
        Get get = new Get(Bytes.toBytes(rowKey(rowkey)));
        // 判断在查询记录时,是否限定列族和列名
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(column));
        }
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)) {
            get.addFamily(Bytes.toBytes(columnfamily));
        }

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        return cells;
    }

    public byte[] getRow1(String table_name, String rowkey, String columnfamily, String column) throws IOException, NoSuchAlgorithmException {

        // table_name和row不能为空
        if (StringUtils.isEmpty(table_name) || StringUtils.isEmpty(rowKey(rowkey))) {
            return null;
        }
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(Bytes.toBytes(rowKey(rowkey)));
        // 判断在查询记录时,是否限定列族和列名
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(column));
        }
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)) {
            get.addFamily(Bytes.toBytes(columnfamily));
        }
        Result result = table.get(get);
        return result.getValue(Bytes.toBytes(columnfamily), Bytes.toBytes(column));
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
     * @param rowkey 主键名
     * @return 表名的MD5散列码
     * @throws NoSuchAlgorithmException,UnsupportedEncodingException
     */
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

    public void close() {
        try {
            if (admin != null) admin.close();
            if (connection != null) connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String rowKey(String rowkey) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        String head = getMD5(rowkey);
        String row = head.substring(0, 2);
        return row + rowkey;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getFamilyName() {
        return this.FamilyName;
    }

}
