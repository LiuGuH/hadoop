package org.apache.hadoop.security.bzl.auth;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenAuthMetrics;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BzlTokenHelper {
  static final Logger LOG = LoggerFactory.getLogger(BzlTokenHelper.class);

  public static String decodeBzlToken(String encodetoken) {
    return new String(Base64.decodeBase64(encodetoken));
  }

  public static ArrayList<String> generateBzlTokenMd5(String user, String timestamp, String period,
      ArrayList<String> passwords) {
    ArrayList<String> list = new ArrayList<>();

    for (int i = 0; i < passwords.size(); i++) {
      String all = String.join(",", user, timestamp, period, passwords.get(i));
      list.add(DigestUtils.md5Hex(all).toUpperCase());
    }

    return list;
  }

  public static boolean authBzlTokenMd5(String user, String bzltokenTimestampe,
      String bzltokenPeriod, String bzltokenMd5, RpcBzlTokenAuthMetrics rpcBzlTokenAuthMetrics) {
    try {
      ArrayList<String> passwords =
          BzlTokenPasswordManager.getInstance().getUserDecodePasswordList(user);
      if (passwords == null || passwords.isEmpty()) {
        LOG.warn(
            "Could not find password from Server with user {}. Check original http data from bzl auth center.",
            user);
        rpcBzlTokenAuthMetrics.incrBzlTokenServerMissingPassword();
        return false;
      }
      List list = generateBzlTokenMd5(user, bzltokenTimestampe, bzltokenPeriod, passwords);
      return list.contains(bzltokenMd5);
    } catch (Exception e) {
      LOG.error("Failed to auth bzl token md5:", e);
      rpcBzlTokenAuthMetrics.incrBzlTokenServerExceptionNumbers();
      return false;
    }
  }
}
