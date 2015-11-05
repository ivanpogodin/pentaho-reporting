package org.pentaho.reporting.engine.classic.core.trial;

import static org.pentaho.reporting.engine.classic.core.layout.output.DebugReporter.DR;

public class Prd5547 {
  public static final Prd5547 inst = new Prd5547();
  boolean processPaginationLevel = false;
  boolean processPage = false;
  int processPageI = -1;
  {
    Thread.currentThread().getStackTrace();
  }
  public void beginProcessPaginationLevel() {
    processPaginationLevel = true;
  }
  public void endProcessPaginationLevel() {
    processPaginationLevel = false;
  }
  public void beginProcessPage() {
    processPage = true;
    processPageI++;
    System.out.println("!!! beginProcessPage [ " + processPageI + " ]");
    DR.printStackTrace(new Throwable(), "--- beginProcessPage [ " + processPageI + " ]");
    //if (processPageI > 1) throw new Error("XXXXXXXXXXXXX");
  }
  public void endProcessPage() {
    processPage = false;
    System.out.println("!!! endProcessPage [ " + processPageI + " ]");
  }
  public boolean isProcessPaginationLevel() {
    return processPaginationLevel;
  }
  public boolean isProcessPage() {
    return processPage;
  }
  public int getProcessPageI() {
    return processPageI;
  }

}
