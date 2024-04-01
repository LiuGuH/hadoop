package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class SlowReadSwitchException extends IOException {
  private static final long serialVersionUID = 1L;
  public SlowReadSwitchException(String description) {
    super(description);
  }
}
