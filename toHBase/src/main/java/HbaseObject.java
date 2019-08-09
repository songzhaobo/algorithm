import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HbaseObject  {
    private Map<String, byte[]> model = null;
    public HbaseObject(HbaseStorage hbaseStorage){
        this.model = new HashMap<>();
    }

    /**
     * 对文件的元数据进行存储
     * @param K
     * @param V
     * */
    public  void setData(String K, byte[] V){
        this.model.put(K,V);
    }

    public  void setData(byte[] V){
        setData("file",V);
    }

    public Set<String> keySet(){
        return model.keySet();
    }

    /**
     * @param name
     * @return 返回对应的内容
     * */
    public byte[] getValues(final String name) {
        return _getValues(name);
    }

    private byte[] _getValues(final String name) {
        byte[] values = model.get(name);
        if (values == null) {
            values = null;
        }
        return values;
    }

}
