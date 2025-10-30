package org.apache.hadoop.security.bzl.util;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.Security;
import java.util.Arrays;

/**
 * Created by dd on 2017/3/9.
 * 数星平台提供的用于密码的加密和解密算法
 */
public final class BzlEncryptionUtils {
  static final Logger LOG = LoggerFactory.getLogger(BzlEncryptionUtils.class);

  private BzlEncryptionUtils() {
  }

  private static final byte[] IV =
      {0x30, 0x31, 0x30, 0x32, 0x30, 0x33, 0x30, 0x34, 0x30, 0x35, 0x30, 0x36, 0x30, 0x37, 0x30,
          0x38};
  private static final String KEY_ALGORITHM = "AES";
  public static final String ALGORITHM_STR = "AES/CBC/PKCS7Padding";
  public static final String KEY_STR = "tiihtNczf1u6AKRyjwEUhQ==";

  private static boolean init = false;
  private static Key key;
  private static Cipher cipher;

  private static void init(byte[] keyBytes) {
    if (!init) {
      init = true;
    } else {
      return;
    }
    int base = 16;
    if (keyBytes.length % base != 0) {
      int groups = keyBytes.length / base + (keyBytes.length % base != 0 ? 1 : 0);
      byte[] temp = new byte[groups * base];
      Arrays.fill(temp, (byte) 0);
      System.arraycopy(keyBytes, 0, temp, 0, keyBytes.length);
      keyBytes = temp;
    }
    Security.addProvider(new BouncyCastleProvider());
    key = new SecretKeySpec(keyBytes, KEY_ALGORITHM);
    try {
      cipher = Cipher.getInstance(ALGORITHM_STR, "BC");
    } catch (Exception e) {
      LOG.warn("BzlEncryptionUtils init error:", e);
    }
  }

  private static byte[] encrypt(byte[] content, byte[] keyBytes) {
    return encryptOfDiyIv(content, keyBytes, IV);
  }

  private static byte[] encryptOfDiyIv(byte[] content, byte[] keyBytes, byte[] ivs) {
    byte[] encryptedText = null;
    init(keyBytes);
    try {
      cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(ivs));
      encryptedText = cipher.doFinal(content);
    } catch (Exception e) {
      LOG.warn("BzlEncryptionUtils encryptOfDiyIv error:", e);
    }
    return encryptedText;
  }

  private static byte[] decryptOfDiyIv(byte[] encryptedData, byte[] keyBytes, byte[] ivs) {
    byte[] encryptedText = null;
    init(keyBytes);
    try {
      cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(ivs));
      encryptedText = cipher.doFinal(encryptedData);
    } catch (Exception e) {
      LOG.warn("BzlEncryptionUtils decryptOfDiyIv error:", e);
    }
    return encryptedText;
  }

  private static byte[] decryptBase64(byte[] key) {
    return Base64.decode(key);
  }

  public static synchronized String encode(String str) {
    return new String(Base64.encode(encrypt(str.getBytes(), decryptBase64(KEY_STR.getBytes()))));
  }

  public static synchronized String decode(String enstr) {
    try {
      return new String(
          decryptOfDiyIv(decryptBase64(enstr.getBytes()), decryptBase64(KEY_STR.getBytes()), IV),
          "UTF-8");
    } catch (Exception e) {
      LOG.warn("BzlEncryptionUtils decode error:", e);
      return null;
    }
  }
}