package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.apache.log4j.Logger;

import java.util.*;

public class HstoreClient extends DB {
  public static final String HOST_PROPERTY = "hstore.addr";
  public static final String USER_PROPERTY = "hstore.user";
  public static final String DEVICE_PROPERTY = "hstore.device";
  private static int SIZE = 1024;
  private final Logger logger = Logger.getLogger(getClass());
  private DawnClient client;

  private static byte[] VALUE=new byte[SIZE];

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String host=props.getProperty(HOST_PROPERTY, "10.0.0.71:11912");
    String username=props.getProperty(USER_PROPERTY, "xuluna");
    String device=props.getProperty(DEVICE_PROPERTY, "mlx5_0");
    client = new DawnClient(username, host, device);
  }

  //Is pool same as table??
  //How do I know the length of the value????
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    int ret = client.get(table, key, VALUE, false);
    if (ret != 0) {
      return Status.ERROR;
    }
    StringByteIterator values = new StringByteIterator(new String(VALUE));
    result.put(fields.iterator().next(), values);
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    byte[] bytes = values.values().iterator().next().toArray();
    return client.put(table, key, bytes, false) == 0 ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return client.erase(table, key) == 0 ? Status.OK : Status.ERROR;
  }

  @Override
  public void cleanup() throws DBException {
    client = null;
    client.clean();
  }
}
