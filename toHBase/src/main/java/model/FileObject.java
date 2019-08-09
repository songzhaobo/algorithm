package model;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class FileObject {

    private Map<String, Byte[]> model ;

    public FileObject(){
        this.model = new HashMap<String, Byte[]>();
    }

    /**
     * 返回model中的Key的集合
     * */
    public String[] getNames() {
        return model.keySet().toArray(new String[model.keySet().size()]);
    }

    /**
     * @param K
     * @param bytes 文件内容的字节数组
     *
     * */
    public void set(String K, Byte[] bytes){
        this.model.put(K,bytes);
    }

/**
 * @param K 字段 如：收件人
 * @param V 字段对应的内容集合  如：收件人1、收件人2、收件人3、
 * */

    /**
     * @param name
     * @return 返回对应的内容
     * */
    public Byte[] getValues(final String name) {
        return _getValues(name);
    }

    private Byte[] _getValues(final String name) {
        Byte[] values = model.get(name);
        if (values == null) {
            values = null;
        }
        return values;
    }


    /**
     * 删除模型中的某一个内容
     * @param name 字段名字
     * */
    public void remove(String name) {
        model.remove(name);
    }


    /**
     * 返回当前模型中存储了多少字段
     * */
    public int size() {
        return model.size();
    }

    /**
     * */
    public Set<String> keySet(){
        return model.keySet();
    }

    @Override
    public String toString() {
        return model.toString();
    }
}