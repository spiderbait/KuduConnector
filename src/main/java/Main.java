import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.LinkedList;

public class Main {

    private static final String TABLE_NAME = "kudu_test.test_20211117";
    String masterAddr;
    KuduClient client;

    public Main(String masterAddr) {
        this.masterAddr = masterAddr;
        this.client = new KuduClient.KuduClientBuilder(masterAddr).defaultAdminOperationTimeoutMs(6000).build();
    }

    public void create() {

        LinkedList<ColumnSchema> columnSchemas = new LinkedList<>();
        columnSchemas.add((new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32)).key(true).build());
        columnSchemas.add((new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING)).key(false).build());
        Schema schema = new Schema(columnSchemas);
        LinkedList<String> parcols = new LinkedList<>();
        parcols.add("id");
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(1);
        options.addHashPartitions(parcols, 3);

        try {
            this.client.createTable(TABLE_NAME, schema, options);
        } catch (KuduException e) {
            try {
                client.close();
            } catch (KuduException ex) {
                ex.printStackTrace();
            }
        }

    }

    public void drop() {
        try {
            this.client.deleteTable(TABLE_NAME);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void insert() {
        try {
            KuduTable table = this.client.openTable(TABLE_NAME);
            KuduSession session = this.client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(3000);

            for (int i=0; i<10; i++) {
                Insert insert = table.newInsert();
                insert.getRow().addInt("id", i);
                insert.getRow().addString("name", i + "NUMBER!");
                session.flush();
                session.apply(insert);
            }
            session.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void query() {
        try {
            KuduTable table = this.client.openTable(TABLE_NAME);
            KuduScanner scanner = client.newScannerBuilder(table).build();
            while (scanner.hasMoreRows()) {
                for (RowResult rowResult : scanner.nextRows()) {
                    System.out.println(rowResult.getInt("id") + "\t" + rowResult.getString("name"));
                }
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("ARGS ERROR! EG.: java -jar KuduConnector.jar example.com:7051 1");
            System.out.println("NOTE: 1 = CREATE, 2 = DROP, 3 = INSERT, 4 = QUERY, operation all for kudu table " + TABLE_NAME + ".");
        } else {
            String masterAddr = args[0];
            int option = Integer.parseInt(args[1]);
            Main main = new Main(masterAddr);
            switch (option) {
                case 1: main.create();break;
                case 2: main.drop();break;
                case 3: main.insert();break;
                case 4: main.query();break;
                default:
                    System.out.println("OPTION ERROR! 1 - 4 ONLY!");
            }
        }

    }
}
