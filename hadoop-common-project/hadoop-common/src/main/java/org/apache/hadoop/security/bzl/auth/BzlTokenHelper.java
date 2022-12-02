package org.apache.hadoop.security.bzl.auth;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.List;

public class BzlTokenHelper {
  public static String decodeBzlToken(String encodetoken) {
    return new String(Base64.decodeBase64(encodetoken));
  }

  public static ArrayList<String> generateBzlToken(String user, String timestamp, String period,
                                                   String[] password) {
    ArrayList<String> list = new ArrayList<>();
    if (password == null || password.length == 0) {
      return list;
    }

    for (int i = 0; i < password.length; i++) {
      String all = String.join(",", user, timestamp, period, password[i]);
      list.add(DigestUtils.md5Hex(all).toUpperCase());
    }
    return list;
  }

  public static boolean authBzlTokenMd5(String user, String bzltokenTimestampe,
                                        String bzltokenPeriod, String bzltokenMd5) {
    String[] password = BzlTokenPasswordManager.getInstance().getUserPasswordList(user);
    List list = generateBzlToken(user, bzltokenTimestampe, bzltokenPeriod, password);
    return list.contains(bzltokenMd5);
  }
}
