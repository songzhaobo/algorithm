/**
 * Created by chenbo on 2018/11/21.
 */
public interface FileStorage {
    /**
     * 获取存储系统的名称
     * @return
     */
    String name();

    /**
     * 获取文件完整限定ID
     * @param id
     * @return
     */
    String qualifiedId(String id);

    /**
     * 判断文件是否存在
     * @param id
     * @return
     */
    boolean exist(String id);


    /**
     * 获取文件对象
     * @param id
     * @return
     */
    FileObject get(String id);
}
