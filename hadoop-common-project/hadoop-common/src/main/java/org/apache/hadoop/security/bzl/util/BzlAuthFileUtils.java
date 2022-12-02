package org.apache.hadoop.security.bzl.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


public class BzlAuthFileUtils {
  static final Logger LOG = LoggerFactory.getLogger(
      BzlAuthFileUtils.class);

  public static ConcurrentHashMap<String, String> readBzlTokenPassword(String fileName) {
    ConcurrentHashMap<String, String> con = new ConcurrentHashMap();
    StringBuffer stb = new StringBuffer();

    BufferedReader br = null;
    String line;

    String md5 = null;
    try {
      br = new BufferedReader(new FileReader(fileName));
      while ((line = br.readLine()) != null) {
        String kvs[] = line.split("=", 2);
        if (!kvs[0].equals("md5")) {
          con.put(kvs[0], kvs[1]);
          stb.append(line + "\n");
        } else {
          md5 = kvs[1];
        }
      }
    } catch (IOException e) {
      LOG.warn("Read {} failed! The error message is: {}.", fileName, e.getMessage());
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Close {} failed! The error message is: {}.", fileName, e.getMessage());
        }
      }
    }

    String remd5 = getContextMd5(stb.toString());

    if (remd5 != null && remd5.equals(md5)) {
      return con;
    }
    LOG.warn("The {} context's md5 is not correct.", fileName);
    return new ConcurrentHashMap();
  }

  public static void writeTokenPasswordToTmpFile(String context, String filename,
                                                 RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics) {
    if (!StringUtils.isNotBlank(context)) {
      return;
    }

    if (!(context.contains("hdfs") && context.contains("yarn") && context.contains("hive"))) {
      return;
    }

    String md5 = getContextMd5(context);
    LOG.debug("write to file {}", filename);
    FileWriter fw = null;
    try {
      fw = new FileWriter(filename);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(context);
      bw.write("md5=" + md5 + "\n");
      bw.flush();
      bw.close();

      if (rpcBzlTokenPasswordFetcherMetrics != null) {
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordWriteTmpFileSuccesses();
      }
    } catch (IOException e) {
      if (rpcBzlTokenPasswordFetcherMetrics != null) {
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordWriteTmpFileFailures();
      }
      LOG.warn("IOException found! The detail is {}", e.getMessage());
    }
  }

  public static String getContextMd5(String context) {
    String md5string = DigestUtils.md5Hex(context);
    return md5string;
  }

  public static void copyTokenPasswordFile(String srcFile, String destFile,
                                           RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics) {
    try {
      Files.copy(Paths.get(srcFile), Paths.get(destFile), REPLACE_EXISTING);

      if (rpcBzlTokenPasswordFetcherMetrics != null) {
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordCopyFileSuccesses();
      }
    } catch (IOException e) {
      LOG.warn("IOException found! The detail is {}.", e.getMessage());
      if (rpcBzlTokenPasswordFetcherMetrics != null) {
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordCopyFileFailures();
      }
    }
  }
}