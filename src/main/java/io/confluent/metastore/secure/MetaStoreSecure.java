package io.confluent.metastore.secure;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.common.HCatUtil;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreSecure {
  private final IMetaStoreClient client;

  public MetaStoreSecure(String hiveConfDir) throws Exception {
    HiveConf hiveConf = new HiveConf(HiveConf.class);
    hiveConf.addResource(new Path(hiveConfDir, "hive-site2.xml"));
    client = HCatUtil.getHiveMetastoreClient(hiveConf);
  }

  public IMetaStoreClient getIMetaStoreClient() {
    return client;
  }

  public static void main(String[] args) {
    try {
      String hiveConfDir = args[0];
      String keytab = args[1];
      String principal = args[2];
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      MetaStoreSecure secure = new MetaStoreSecure(hiveConfDir);
      IMetaStoreClient client = secure.getIMetaStoreClient();

      Table table = new Table("default", "test");
      table.setTableType(TableType.MANAGED_TABLE);
      List<FieldSchema> columns = new ArrayList<>();
      FieldSchema fieldSchema = new FieldSchema("a", TypeInfoFactory.intTypeInfo.getTypeName(), "");
      columns.add(fieldSchema);
      table.setFields(columns);

      client.createTable(table.getTTable());
      table = new Table(client.getTable("default", "test"));

      System.out.println(table.getDbName());
      System.out.println(table.getTableName());
      for (FieldSchema schema : table.getCols()) {
        System.out.println(schema.getName());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
