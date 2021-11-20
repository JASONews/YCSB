package site.ycsb.db;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * This code is derived from MemcachedClient.java
 */

public class ZiplogClient extends DB {

  private final Logger logger = Logger.getLogger(getClass());
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private ZiplogClientJNI client;

  @Override
  public void init() throws DBException {

    client = new ZiplogClientJNI();
    // try {
    // client = ZiplogClientJNI();
    // checkOperationStatus = Boolean.parseBoolean(
    // getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY,
    // CHECK_OPERATION_STATUS_DEFAULT));
    // objectExpirationTime = Integer.parseInt(
    // getProperties().getProperty(OBJECT_EXPIRATION_TIME_PROPERTY,
    // DEFAULT_OBJECT_EXPIRATION_TIME));
    // shutdownTimeoutMillis = Integer.parseInt(
    // getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY,
    // DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
    // } catch (Exception e) {
    // throw new DBException(e);
    // }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    Object document = client.get(key);
    if (document != null) {
      try {
        fromJson((String) document, fields, result);
        return Status.OK;
      } catch (Exception ex) {
      }
    }
    logger.error("Error encountered for key: " + key);
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
    // key = createQualifiedKey(table, key);
    // try {
    // OperationFuture<Boolean> future =
    // memcachedClient().replace(key, objectExpirationTime, toJson(values));
    // return getReturnCode(future);
    // } catch (Exception e) {
    // logger.error("Error updating value with key: " + key, e);
    // return Status.ERROR;
    // }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      if (client.put(key, toJson(values))) {
        return Status.OK;
      }
    } catch (Exception ex) {
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
    // key = createQualifiedKey(table, key);
    // try {
    // OperationFuture<Boolean> future = memcachedClient().delete(key);
    // return getReturnCode(future);
    // } catch (Exception e) {
    // logger.error("Error deleting value", e);
    // return Status.ERROR;
    // }
  }

  @Override
  public void cleanup() throws DBException {
    // if (client != null) {
    // memcachedClient().shutdown(shutdownTimeoutMillis, MILLISECONDS);
    // }
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result)
      throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext();
    /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && !fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values) throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }

}
