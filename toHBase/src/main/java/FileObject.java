import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/**
 * Created by chenbo on 2018/11/21.
 */
public interface FileObject {
    /**
     * 获取文件名
     * @return
     */
    String getName();

    /**
     * 获取文件长度
     * @return
     */
    long length();

    /**
     * 输出到流中
     * @param outputStream
     */
    void writeTo(OutputStream outputStream);

    /**
     * 获取内容
     * @return
     */
    default byte[] getContent() {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        writeTo(byteOutputStream);
        return byteOutputStream.toByteArray();
    }

}
