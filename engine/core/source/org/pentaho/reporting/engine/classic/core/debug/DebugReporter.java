package org.pentaho.reporting.engine.classic.core.debug;

import static org.pentaho.reporting.engine.classic.core.debug.DebugReporter.DR;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.pentaho.reporting.engine.classic.core.layout.AbstractRenderer;
import org.pentaho.reporting.engine.classic.core.layout.model.LogicalPageBox;
import org.pentaho.reporting.engine.classic.core.layout.model.RenderNode;
import org.pentaho.reporting.engine.classic.core.layout.output.DefaultOutputFunction;

public class DebugReporter {
  public static final DebugReporter DR = new DebugReporter();

  public static boolean ON = true;
  public static final Boolean ON_SetY = false;
  public final int maxNameLength = 187;
  public static boolean isDisplayed(int i, String msg) {
    //return false;
    //return i > 1300 && i < 2500;
    return true;
  }
  

  int i = 0;
  int j = 0;
  File dir;
  final DecimalFormat idxFormat = new DecimalFormat("0000");

  {
    final String txtNow = ( new SimpleDateFormat( "yyyy-MM-dd_HH-mm-ss" ) ).format( Calendar.getInstance().getTime() );
    dir = new File( "D:\\PENT\\work\\PRD-5547\\debug-callstacks-" + txtNow + "\\" );
    dir.mkdirs();
  }

  int nextI() {
    return ++i;
  }
  int nextJ() {
    return ++j;
  }

  public void printStackTrace( Throwable th, String msg ) {
    printStackTrace( th, msg, false );
  }

  public void printStackTrace( Throwable th, String msg, boolean currentI ) {
    if (!ON)return;
    FileOutputStream outS = null;
    try {
      outS = getFileOutStream( msg, currentI, ".trace.txt" );
      if (outS==null) return;
      PrintStream out = new PrintStream( outS );
      th.printStackTrace( out );
    } catch ( FileNotFoundException e ) {
      e.printStackTrace();
    } finally {
      if ( outS != null ) {
        try {
          outS.close();
        } catch ( IOException e1 ) {
          e1.printStackTrace();
        }
      }
    }
  }

  private FileOutputStream getFileOutStream( String msg, boolean currentI, String suffix  ) throws FileNotFoundException {
    if (suffix==null) {
      suffix = "";
    }
    FileOutputStream outS;
    int idx;
    String fileName;
    if ( !currentI ) {
      idx = nextI();
      j = 0;
      fileName = idxFormat.format( idx ) + ".0_" + msg + suffix;
    } else {
      idx = i;
      int idx2 = nextJ();
      fileName = idxFormat.format( idx ) + "." +idx2+ "_" + msg + suffix;
    }
    
    
    fileName = fileName.replaceAll(":", "_").replaceAll("=", "~");
    
    
    if (fileName.length() > maxNameLength) {
      fileName = fileName.substring( 0, maxNameLength-suffix.length() ) + suffix;
    }
    if (!isDisplayed(i, msg)) {
      System.out.println("* " + fileName);
      return null;
    }
    System.out.println(fileName);
    File outFile = new File( dir, fileName );
    outS = new FileOutputStream( outFile );
    try {
      OutputStreamWriter wr = new OutputStreamWriter( outS, "UTF-8" );
      wr.write( msg );
      wr.write( "\r\n----\r\n" );
      wr.flush();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return outS;
  }
  

  public void printNode( RenderNode node, String msg ) {
    printNode( node, msg, false );
  }

  public void printNode( RenderNode node, String msg, boolean currentI ) {
    if (!ON)return;
    FileOutputStream outS = null;
    try {
      outS = getFileOutStream( msg, currentI, ".node.txt" );
      if (outS==null) return;
      PrintStream out = new PrintStream( outS );
      OutputStreamWriter wr = new OutputStreamWriter( out, "UTF-8" );
      wr.write( DebugUtil.display( node ) );
      wr.flush();
    } catch ( Exception e ) {
      e.printStackTrace();
      System.out.println( "!!!!!!!!!!!!!!!!!!!!" );
      System.exit( -1 );
    } finally {
      if ( outS != null ) {
        try {
          outS.close();
        } catch ( IOException e1 ) {
          e1.printStackTrace();
        }
      }
    }
  }
  public void printText( String text, String msg) {
    printText( text, msg, false);
  }
  public void printText( String text, String msg, boolean currentI ) {
    if (!ON)return;
    FileOutputStream outS = null;
    try {
      outS = getFileOutStream( msg, currentI, ".node.txt" );
      if (outS==null) return;
      PrintStream out = new PrintStream( outS );
      OutputStreamWriter wr = new OutputStreamWriter( out, "UTF-8" );
      wr.write( text );
      wr.flush();
      wr.close();
    } catch ( Exception e ) {
      e.printStackTrace();
    } finally {
      if ( outS != null ) {
        try {
          outS.close();
        } catch ( IOException e1 ) {
          e1.printStackTrace();
        }
      }
    }
  }
  
  public void printTraceAndNode(Throwable th, RenderNode node, String msg) {
    DR.printStackTrace( th, msg);
    DR.printNode( node, msg, true);
  }
}
