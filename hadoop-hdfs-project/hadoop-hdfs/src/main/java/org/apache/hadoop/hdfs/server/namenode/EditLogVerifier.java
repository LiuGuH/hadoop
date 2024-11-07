package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

public class EditLogVerifier {

  public static void main(String[] args) throws IOException {
    scan(args[0]);
  }

  private static void scan(String fileName) throws IOException {
    long startTxId = HdfsServerConstants.INVALID_TXID;
    long endTxId = HdfsServerConstants.INVALID_TXID;
    long validBytes = 0;
    boolean hasException = false;

    File file = new File(fileName);
    EditLogFileInputStream inputStream = new EditLogFileInputStream(file);
    try {
      inputStream.getVersion(false);
    } catch (IOException e) {
      hasException = true;
      System.out.print(
          "{\"startTxId\":\"" + startTxId + "\",\"endTxId\":\"" + endTxId + "\",\"validBytes\":\""
              + validBytes + "\",\"hasException\":\"" + hasException + "\"}");
      return;
    }

    while (true) {
      validBytes = inputStream.getPosition();
      long txid;
      try {
        if ((txid = inputStream.scanNextOp()) == HdfsServerConstants.INVALID_TXID) {
          break;
        }
      } catch (Throwable t) {
        hasException = true;
        break;
      }
      if (startTxId == HdfsServerConstants.INVALID_TXID) {
        startTxId = txid;
      }
      if (endTxId == HdfsServerConstants.INVALID_TXID || txid > endTxId) {
        endTxId = txid;
      }
    }
    System.out.print(
        "{\"startTxId\":\"" + startTxId + "\",\"endTxId\":\"" + endTxId + "\",\"validBytes\":\""
            + validBytes + "\",\"hasException\":\"" + hasException + "\"}");
  }
}
