package model;

import java.util.HashMap;

/**
 * RDB类数据来源，使用时需要初始化该类，将数据导入该模型
 * @author 中科天玑
 * @Date 2018.8.13
 * */
public class RDBSourse extends HashMap implements RDBModel {
    /**
     * @param tablemetadata 表的字段    如：   学号
     * @param tabledata  元数据对应的数据 如：  12
     * */
    @Override

    @SuppressWarnings("unchecked")
    public void setTableDataToModel(Object tablemetadata, Object tabledata) {

        super.put(tablemetadata,tabledata);
    }

    /**
     *
     * @param tablename 表名
     * @param tablemeta_data 表对应的字段集合
     * */
    @Override
    @SuppressWarnings("unchecked")
    public void setTableNameTablemeta(Object tablename, RDBModel tablemeta_data) {
        super.put(tablename,tablemeta_data);
    }

    /**
     * @param dbname  数据库名字
     * @param table_tabledata  表集合
     *
     * */
    @Override
    @SuppressWarnings("unchecked")
    public void setDBNameTableName(Object dbname, RDBModel table_tabledata) {

        super.put(dbname,table_tabledata);
    }



}
