package site.ycsb.db;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

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
 * This code is derived from MemcachedClient.java.
 */

public class ZiplogClient extends DB {

  private final Logger logger = Logger.getLogger(getClass());
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private boolean useLocal; 
  private boolean islogToScreen; 
  private String outputDir;
  private String outputFileExt;

  private ZiplogClientJNI client;
  private static ConcurrentHashMap<Long, List<String>> ops = new ConcurrentHashMap<>();
  private static ConcurrentLinkedDeque<List<String>> outputOps = new ConcurrentLinkedDeque<>();
  

  @Override
  public void init() throws DBException {
    String serverIp = getProperties().getProperty("ServerIp", "192.168.99.18");
    int serverPort = Integer.parseInt(getProperties().getProperty("ServerPort", "12500"));
    int shard = Integer.parseInt(getProperties().getProperty("Shard", "0"));
    int clientId = Integer.parseInt(getProperties().getProperty("ClientId", "1"));
    useLocal = Boolean.parseBoolean(getProperties().getProperty("ziplog.useLocal", "false"));
    islogToScreen = Boolean.parseBoolean(getProperties().getProperty("ziplog.logToScreen", "false"));
    outputDir = getProperties().getProperty("ziplog.outputDir", ".");
    outputFileExt = getProperties().getProperty("ziplog.outputFileExt", ".put");

    System.err.println("Client id is " + clientId);
    System.err.println("useLocal is " + useLocal);

    if (!useLocal) {
      client = new ZiplogClientJNI(serverIp, serverPort, shard, clientId);
    } else {
      long tid = Thread.currentThread().getId();
      System.err.println("Thread id is " + tid);
      ops.put(tid, new ArrayList<>());
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    long tid = Thread.currentThread().getId();
    key = createQualifiedKey(table, key);
    if (useLocal) {
      logToScreen(tid, "GET", key);
      logToFile(tid, "GET", key);
      return Status.OK;

    } else {
      Object document = client.get(key);
      if (document != null) {
        try {
          fromJson((String) document, fields, result);
          return Status.OK;
        } catch (Exception ex) {
          System.err.println(ex.getLocalizedMessage());
          ex.printStackTrace();
        }
      }
      System.err.println("Error encountered for key: " + key);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    long tid = Thread.currentThread().getId();
    key = createQualifiedKey(table, key);
    try {
      String value = toJson(values);
      if (useLocal) {
        logToScreen(tid, "PUT", String.format("%s %s", key, value));
        logToFile(tid, "PUT", String.format("%s %s", key, value));
        return Status.OK;
      } else {
        if (client.put(key, value)) {
          return Status.OK;
        } else {
          return Status.ERROR;
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public void cleanup() throws DBException {
    if (useLocal) {
      long tid = Thread.currentThread().getId();
      outputOps.add(ops.get(tid));

      if (outputOps.size() == ops.size()) {

        while (!outputOps.isEmpty()) {
          List<String> opl = outputOps.pop();
          try {
            File f = new File(outputDir, "Client_" + outputOps.size() + outputFileExt);
            System.err.printf("Log to file %s\n", f.getAbsolutePath());
            FileWriter fs = new FileWriter(f);
            for (String op : opl) {
              fs.write(op);
            }
            fs.close();
          } catch (IOException ex) {
            ex.printStackTrace();
          }
        }
      }
    }
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

  protected void logToScreen(long tid, String op, String message) {
    if (islogToScreen) {
      try {
        System.out.println(String.format("%d %s %s", tid, op, message));
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  protected void logToFile(long tid, String op, String message) {
    ops.get(tid).add(String.format("%s %s\n", op, message));
  }
}
