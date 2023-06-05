package org.apache.hadoop.hdfs.server.namenode;

/**
 * As a extension properties used in audit log.
 * It could contain some informations such as the affected block number of rpc..
 * the parttern as below:
 * extensionInfo=numBlocks:1250,xxx:yyy
 */
public class ExtensionInfo {
  private int numBlocks;
  private int numFiles;

  public ExtensionInfo() {
    numBlocks = 0;
    numFiles = 0;
  }

  public static class Builder {
    private int numBlocks;
    private int numFiles;

    public Builder() {
      this.numBlocks = 0;
      this.numFiles = 0;
    }

    public Builder blocksCount(int numBlocks) {
      this.numBlocks = numBlocks;
      return this;
    }

    public Builder filesCount(int numFiles) {
      this.numFiles = numFiles;
      return this;
    }

    public ExtensionInfo build() {
      ExtensionInfo extensionInfo = new ExtensionInfo();
      extensionInfo.setNumBlocks(this.numBlocks);
      extensionInfo.setNumFiles(this.numFiles);
      return extensionInfo;
    }

  }

  public int getNumBlocks() {
    return numBlocks;
  }

  public int getNumFiles() {
    return numFiles;
  }

  public void setNumBlocks(int numBlocks) {
    this.numBlocks = numBlocks;
  }

  public void setNumFiles(int numFiles) {
    this.numFiles = numFiles;
  }

  public int getValueForCmd(String cmd) {
    if (cmd == null) {
      return 0;
    }

    if (cmd.equals("listStatus")) {
      return getNumFiles();
    }

    if (cmd.equals("delete")) {
      return getNumBlocks();
    }

    return 1;
  }

  public boolean isValid() {
    return getNumFiles() > 0 || getNumBlocks() > 0;
  }
}
