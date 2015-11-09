package org.pentaho.reporting.engine.classic.core.debug;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Envelopes a Reader.
 * Closed automatically when becomes not able to read data (end is reached). 
 *
 */
public class AutocloseReader extends FilterReader{

  public AutocloseReader( Reader in ) {
    super( in );
  }

  @Override
  public int read() throws IOException {
    int ret = super.read();
    if (ret == -1) {
      in.close();
    }
    return ret;
  }

  @Override
  public int read( char[] cbuf, int off, int len ) throws IOException {
    int ret = super.read( cbuf, off, len );
    if (ret == 0) {
      in.close();
    }
    return ret;
  }


}
