package model;

/**
 * RDB的数据模版
 * @author 中科天玑
 * @Date 2018.8.13
 * */

public interface RDBModel<F,SF>  {

    /**
     * @param tablemetadata 表的字段    如： 学号
     * @param tabledata  元数据对应的数据 如：12
     * */
    void setTableDataToModel(F tablemetadata, SF tabledata);

    /**
     * @param tablename 表名
     * @param tablemeta_data 表对应的字段集合
     * */
    void setTableNameTablemeta(F tablename, RDBModel tablemeta_data);

    /**
     * @param dbname  数据库名字
     * @param table_tabledata  表集合
     * */
    void setDBNameTableName(F dbname, RDBModel table_tabledata);
}

