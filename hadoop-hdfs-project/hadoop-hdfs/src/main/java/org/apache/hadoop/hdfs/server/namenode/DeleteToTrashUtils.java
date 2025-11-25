package org.apache.hadoop.hdfs.server.namenode;

import java.util.SortedSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_ENABLE;

public class DeleteToTrashUtils {

  public static final String DELIMITER = String.valueOf(Path.SEPARATOR_CHAR);
  public static final String CURRENT = "/Current";
  public static final String NN = "/.NN";
  public static final String HIVE_STAGING_DIR = ".hive-staging";
  public static final String SPARK_STAGING_DIR = ".spark-staging";
  public static final String TEMPORARY_DIR = "_temporary";



  public static String getTrashRoot() {
    // Gets the Trash directory for the current user.
    return FileSystem.USER_HOME_PREFIX + DELIMITER + Server.getRemoteUser().getUserName()
        + DELIMITER + FileSystem.TRASH_PREFIX;
  }

  // src must not be endwith '/'
  public static String getTrashPath(String src) {
    return getTrashRoot() + CURRENT + NN + src;
  }

  // src must not be endwith '/'
  public static String getBaseTrashPath(String src) {
    return getTrashRoot() + CURRENT + NN + getPathParent(src);
  }

  public static String getPathParent(String path) {
    int lastIndex = path.lastIndexOf(DELIMITER);
    return path.substring(0, lastIndex);
  }

  public static String getPathLast(String path) {
    int lastIndex = path.lastIndexOf(DELIMITER);
    return path.substring(lastIndex);
  }

  public static boolean checkIfDeleteToTrash(FSDirectory fsd, String src) {
    if (!BzlForceToTrashGlobalEnableThread.isGlobalEnable()) {
      return false;
    }

    if (!BzlDynamicConfiguration.getInstance()
        .getBoolean(FS_FORCE_TO_TRASH_BZL_ENABLE, false)) {
      return false;
    }

    if (src.contains(FileSystem.TRASH_PREFIX) || src.contains(HIVE_STAGING_DIR) || src.contains(
        SPARK_STAGING_DIR) || src.contains(TEMPORARY_DIR)) {
      return false;
    }

    long start = Time.monotonicNowNanos();
    try {
      SortedSet<String> forceToTrashDirs = fsd.getForceToTrashDirectories();
      if (forceToTrashDirs.isEmpty()) {
        return false;
      }

      if (forceToTrashDirs.contains(src)) {
        return true;
      }

      while (!src.isEmpty()) {
        int index = src.lastIndexOf(DELIMITER);
        src = src.substring(0, index);
        if (forceToTrashDirs.contains(src)) {
          return true;
        }
      }
      return false;
    } finally {
      BzlForceToTrashDirectoriesUpdater.getInstance()
          .getBzlForceToTrashDirectoriesProcessingTime().add(Time.monotonicNowNanos() - start);
    }
  }
}
