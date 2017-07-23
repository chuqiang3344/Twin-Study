package com.tyaer.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class demo1 {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
