package com.tyaer.strom.hbase;

import com.tyaer.database.hbase.HBaseHelper;
import com.tyaer.database.hbase.bean.RowBean;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Twin on 2017/6/20.
 */
public class Hbase_Test {
    static HBaseHelper hBaseHelpe=new HBaseHelper();

    public static void main(String[] args) {
        String tab11 = "zcq_test";
        int tab1 = hBaseHelpe.countTableRecords(tab11);
        System.out.println(tab1);
        ArrayList<RowBean> rowBeans = new ArrayList<>();
        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("name","Zxc");
        RowBean rowBean = new RowBean("33322", "fn", hashMap);
        rowBeans.add(rowBean);
        hBaseHelpe.insertRows(tab11,rowBeans);
        System.out.println(hBaseHelpe.countTableRecords(tab11));

        hBaseHelpe.closeHbaseConnection();
    }

    @Test
    public void insert() {
        HashMap<String, String> map = new HashMap<>();
        map.put("name", "周楚强");
        map.put("age", "20");
//        hBaseHelper.addRow("zcq_test","1234","cf1",map);
        String tableName = "zcq_test";
        long row = 45465465465454564L;
        for (int i = 0; i < 100; i++) {
            row++;
            hBaseHelpe.updateRow(tableName, row+"", "fn", map);
        }
//        System.out.println(hBaseHelper.getRow(tableName, row));
//        new List<>()
//        hBaseHelper.insertRows("zcq_test","1234","cf1",map);
    }
}
