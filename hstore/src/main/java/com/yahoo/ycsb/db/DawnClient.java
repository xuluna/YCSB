package com.yahoo.ycsb.db;

class DawnClient {

  static {
    System.loadLibrary("dawn-client");
  }

  //addr should include port
  public DawnClient(String username, String addr, String device) {
    this(2, username, addr, device);
  }

  //addr should include port
  public DawnClient(int debug_level, String username, String addr, String device) {
    init(debug_level, username, addr, device);
  }

  private native void init(int debug_level, String username, String addr, String device);

  //what is the value size that will trigger direct??
  public native int put(String table, String key, byte[] value, boolean direct);

  public native int get(String table, String key, byte[] value, boolean direct);

  public native int erase(String table, String key);

  public native int clean();

  public static void main(String[] args){
    new DawnClient("aa", "10.0.0.71:11912","mlx5_0");
  }

}
