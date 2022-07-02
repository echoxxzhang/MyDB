package com.echoxxzhang.mydb.backend.utils;

public class Panic {
  public static void panic(Exception err) {
    err.printStackTrace(); // 打印日志
    System.exit(1); // 退出程序
  }
}
