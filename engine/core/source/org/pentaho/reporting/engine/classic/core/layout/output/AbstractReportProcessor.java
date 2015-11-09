/*
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2001 - 2013 Object Refinery Ltd, Pentaho Corporation and Contributors..  All rights reserved.
*/

package org.pentaho.reporting.engine.classic.core.layout.output;

import static org.pentaho.reporting.engine.classic.core.debug.DebugReporter.DR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.reporting.engine.classic.core.ClassicEngineBoot;
import org.pentaho.reporting.engine.classic.core.ClassicEngineCoreModule;
import org.pentaho.reporting.engine.classic.core.EmptyReportException;
import org.pentaho.reporting.engine.classic.core.MasterReport;
import org.pentaho.reporting.engine.classic.core.PerformanceTags;
import org.pentaho.reporting.engine.classic.core.ReportEventException;
import org.pentaho.reporting.engine.classic.core.ReportInterruptedException;
import org.pentaho.reporting.engine.classic.core.ReportProcessingException;
import org.pentaho.reporting.engine.classic.core.debug.AutocloseReader;
import org.pentaho.reporting.engine.classic.core.debug.BSh;
import org.pentaho.reporting.engine.classic.core.debug.DebugReporter;
import org.pentaho.reporting.engine.classic.core.debug.Prd5547;
import org.pentaho.reporting.engine.classic.core.event.ReportProgressEvent;
import org.pentaho.reporting.engine.classic.core.event.ReportProgressListener;
import org.pentaho.reporting.engine.classic.core.function.OutputFunction;
import org.pentaho.reporting.engine.classic.core.layout.AbstractRenderer;
import org.pentaho.reporting.engine.classic.core.layout.Renderer;
import org.pentaho.reporting.engine.classic.core.layout.model.LogicalPageBox;
import org.pentaho.reporting.engine.classic.core.states.CollectingReportErrorHandler;
import org.pentaho.reporting.engine.classic.core.states.IgnoreEverythingReportErrorHandler;
import org.pentaho.reporting.engine.classic.core.states.LayoutProcess;
import org.pentaho.reporting.engine.classic.core.states.PerformanceMonitorContext;
import org.pentaho.reporting.engine.classic.core.states.ProcessStateHandle;
import org.pentaho.reporting.engine.classic.core.states.ReportProcessingErrorHandler;
import org.pentaho.reporting.engine.classic.core.states.ReportStateKey;
import org.pentaho.reporting.engine.classic.core.states.process.PendingPagesHandler;
import org.pentaho.reporting.engine.classic.core.states.process.ProcessState;
import org.pentaho.reporting.engine.classic.core.states.process.RestartOnNewPageHandler;
import org.pentaho.reporting.engine.classic.core.util.IntList;
import org.pentaho.reporting.libraries.base.config.Configuration;
import org.pentaho.reporting.libraries.base.util.MemoryUsageMessage;
import org.pentaho.reporting.libraries.base.util.PerformanceLoggingStopWatch;
import org.pentaho.reporting.libraries.docbundle.DocumentBundle;
import org.pentaho.reporting.libraries.docbundle.DocumentMetaData;
import org.pentaho.reporting.libraries.docbundle.MemoryDocumentMetaData;
import org.pentaho.reporting.libraries.xmlns.common.ParserUtil;

/**
 * @noinspection HardCodedStringLiteral
 */
public abstract class AbstractReportProcessor implements ReportProcessor {
  private static final Log logger = LogFactory.getLog( AbstractReportProcessor.class );
  private static final boolean SHOW_ROLLBACKS = logger.isTraceEnabled();

  protected static final int MAX_EVENTS_PER_RUN = 200;
  protected static final int MIN_ROWS_PER_EVENT = 100;
  protected static final int COMMIT_RATE = 1;

  /**
   * A flag defining whether to check for Thread-Interrupts.
   */
  private boolean handleInterruptedState;

  /**
   * Storage for listener references.
   */
  private ArrayList<ReportProgressListener> listeners;

  /**
   * The listeners as object array for faster access.
   */
  private transient Object[] listenersCache;

  private MasterReport report;

  private OutputProcessor outputProcessor;

  private PageStateList stateList;
  private transient ProcessStateHandle activeDataFactory;

  private IntList physicalMapping;
  private IntList logicalMapping;
  private boolean paranoidChecks;
  private PerformanceMonitorContext performanceMonitorContext;

  /**
   * A flag controlling whether we save page-states to make browsing pages easier and faster.
   */
  private boolean fullStreamingProcessor;
  /**
   * An internal flag that is only valid after the pagination has started.
   */
  private boolean pagebreaksSupported;

  protected AbstractReportProcessor( final MasterReport report,
                                     final OutputProcessor outputProcessor )
    throws ReportProcessingException {
    if ( report == null ) {
      throw new NullPointerException( "Report cannot be null." );
    }

    if ( outputProcessor == null ) {
      throw new NullPointerException( "OutputProcessor cannot be null" );
    }

    // first cloning ... protect the page layouter function ...
    // and any changes we may do to the report instance.

    // a second cloning is done in the start state, to protect the
    // processed data.
    this.fullStreamingProcessor = true;
    this.handleInterruptedState = true;
    this.outputProcessor = outputProcessor;

    final Configuration configuration = report.getReportConfiguration();
    this.paranoidChecks = "true".equals( configuration.getConfigProperty
      ( "org.pentaho.reporting.engine.classic.core.layout.ParanoidChecks" ) );
    final String yieldRateText =
      configuration.getConfigProperty( "org.pentaho.reporting.engine.classic.core.YieldRate" );
    final int yieldRate = ParserUtil.parseInt( yieldRateText, 0 );
    if ( yieldRate > 0 ) {
      addReportProgressListener( new YieldReportListener( yieldRate ) );
    }
    final String profile = configuration.getConfigProperty
      ( "org.pentaho.reporting.engine.classic.core.ProfileReportProcessing" );
    if ( "true".equals( profile ) ) {
      final boolean logLevelProgress = "true".equals
        ( configuration.getConfigProperty( "org.pentaho.reporting.engine.classic.core.performance.LogLevelProgress" ) );
      final boolean logPageProgress = "true".equals
        ( configuration.getConfigProperty( "org.pentaho.reporting.engine.classic.core.performance.LogPageProgress" ) );
      final boolean logRowProgress = "true".equals
        ( configuration.getConfigProperty( "org.pentaho.reporting.engine.classic.core.performance.LogRowProgress" ) );
      addReportProgressListener( new PerformanceProgressLogger( logLevelProgress, logPageProgress, logRowProgress ) );
    }

    final boolean designtime = outputProcessor.getMetaData().isFeatureSupported( OutputProcessorFeature.DESIGNTIME );
    this.report = report.derive( designtime );
  }

  protected ProcessStateHandle getProcessStateHandle() {
    return activeDataFactory;
  }

  protected MasterReport getReport() {
    return report;
  }

  public OutputProcessor getOutputProcessor() {
    return outputProcessor;
  }

  protected OutputProcessorMetaData getOutputProcessorMetaData() {
    return outputProcessor.getMetaData();
  }

  private PerformanceMonitorContext getPerformanceMonitorContext() {
    if ( this.performanceMonitorContext == null ) {
      this.performanceMonitorContext =
        ClassicEngineBoot.getInstance().getObjectFactory().get( PerformanceMonitorContext.class );
    }
    return performanceMonitorContext;
  }

  /**
   * Adds a repagination listener. This listener will be informed of pagination events.
   *
   * @param l the listener.
   */
  public void addReportProgressListener( final ReportProgressListener l ) {
    if ( l == null ) {
      throw new NullPointerException( "Listener == null" );
    }
    if ( listeners == null ) {
      listeners = new ArrayList<ReportProgressListener>( 5 );
    }
    listenersCache = null;
    listeners.add( l );
  }

  /**
   * Removes a repagination listener.
   *
   * @param l the listener.
   */
  public void removeReportProgressListener( final ReportProgressListener l ) {
    if ( l == null ) {
      throw new NullPointerException( "Listener == null" );
    }
    if ( listeners == null ) {
      return;
    }
    listenersCache = null;
    listeners.remove( l );
  }

  /**
   * Sends a repagination update to all registered listeners.
   *
   * @param state the state.
   */
  protected void fireStateUpdate( final ReportProgressEvent state ) {
    if ( listeners == null ) {
      return;
    }
    if ( listenersCache == null ) {
      listenersCache = listeners.toArray();
    }
    final int length = listenersCache.length;
    for ( int i = 0; i < length; i++ ) {
      final ReportProgressListener l = (ReportProgressListener) listenersCache[ i ];
      l.reportProcessingUpdate( state );
    }
  }

  /**
   * Sends a repagination update to all registered listeners.
   *
   * @param state the state.
   */
  protected void fireProcessingStarted( final ReportProgressEvent state ) {
    if ( listeners == null ) {
      return;
    }
    if ( listenersCache == null ) {
      listenersCache = listeners.toArray();
    }
    final int length = listenersCache.length;
    for ( int i = 0; i < length; i++ ) {
      final ReportProgressListener l = (ReportProgressListener) listenersCache[ i ];
      l.reportProcessingStarted( state );
    }
  }

  /**
   * Sends a repagination update to all registered listeners.
   *
   * @param state the state.
   */
  protected void fireProcessingFinished( final ReportProgressEvent state ) {
    if ( listeners == null ) {
      return;
    }
    if ( listenersCache == null ) {
      listenersCache = listeners.toArray();
    }
    final int length = listenersCache.length;
    for ( int i = 0; i < length; i++ ) {
      final ReportProgressListener l = (ReportProgressListener) listenersCache[ i ];
      l.reportProcessingFinished( state );
    }
  }

  /**
   * Returns whether the processor should check the threads interrupted state. If this is set to true and the thread was
   * interrupted, then the report processing is aborted.
   *
   * @return true, if the processor should check the current thread state, false otherwise.
   */
  public boolean isHandleInterruptedState() {
    return handleInterruptedState;
  }

  /**
   * Defines, whether the processor should check the threads interrupted state. If this is set to true and the thread
   * was interrupted, then the report processing is aborted.
   *
   * @param handleInterruptedState true, if the processor should check the current thread state, false otherwise.
   */
  public void setHandleInterruptedState( final boolean handleInterruptedState ) {
    this.handleInterruptedState = handleInterruptedState;
  }

  /**
   * Checks whether the current thread is interrupted.
   *
   * @throws org.pentaho.reporting.engine.classic.core.ReportInterruptedException if the thread is interrupted to abort
   *                                                                              the report processing.
   */
  protected final void checkInterrupted()
    throws ReportInterruptedException {
    if ( isHandleInterruptedState() ) {
      if ( Thread.currentThread().isInterrupted() ) {
        throw new ReportInterruptedException(
          "Current thread [" + Thread.currentThread().getName() + "]is interrupted. Returning." );
      }
    }
  }

  public synchronized void close() {
    if ( activeDataFactory != null ) {
      this.activeDataFactory.close();
      this.activeDataFactory = null;
      this.stateList = null;
      this.physicalMapping = null;
      this.logicalMapping = null;
    }
  }

  public Configuration getConfiguration() {
    return report.getConfiguration();
  }

  protected DefaultProcessingContext createProcessingContext() throws ReportProcessingException {
    final OutputProcessorMetaData metaData = getOutputProcessorMetaData();
    final MasterReport report = getReport();
    final DocumentMetaData documentMetaData;
    final DocumentBundle bundle = report.getBundle();
    if ( bundle != null ) {
      documentMetaData = bundle.getMetaData();
    } else {
      documentMetaData = new MemoryDocumentMetaData();
    }
    final Integer compatibilityLevel = report.getCompatibilityLevel();
    final int cLevel;
    if ( compatibilityLevel == null ) {
      cLevel = -1;
    } else {
      cLevel = compatibilityLevel;
    }
    return new DefaultProcessingContext
      ( metaData, report.getResourceBundleFactory(), report.getConfiguration(),
        report.getResourceManager(), report.getContentBase(), documentMetaData,
        report.getReportEnvironment(), cLevel );
  }

  /**
   * Processes all prepare levels to compute the function values.
   *
   * @param state   the state state with which we beginn the processing.
   * @param maxRows the number of rows in the table model.
   * @return the finish state for the current level.
   * @throws ReportProcessingException if processing failed or if there are exceptions during the function execution.
   */
  protected ProcessState processPrepareLevels( ProcessState state, final int maxRows )
    throws ReportProcessingException {
    final boolean failOnError = isStrictErrorHandling( getReport().getReportConfiguration() );
    final ReportProcessingErrorHandler errorHandler = new CollectingReportErrorHandler();
    state.setErrorHandler( errorHandler );

    int lastRow = -1;
    int eventCount = 0;
    final int eventTrigger;
    if ( maxRows <= 0 ) {
      eventTrigger = Math.max( maxRows / MAX_EVENTS_PER_RUN, MIN_ROWS_PER_EVENT );
    } else {
      eventTrigger = Math.min( maxRows, Math.max( maxRows / MAX_EVENTS_PER_RUN, MIN_ROWS_PER_EVENT ) );
    }

    final ReportProgressEvent repaginationState = new ReportProgressEvent( this );
    // Function processing does not use the PageLayouter, so we don't need
    // the expensive cloning ...
    while ( !state.isFinish() ) {
      checkInterrupted();
      if ( lastRow != state.getCurrentRow() ) {
        lastRow = state.getCurrentRow();
        if ( eventCount == 0 ) {
          repaginationState.reuse( ReportProgressEvent.PRECOMPUTING_VALUES, state, 0 );
          fireStateUpdate( repaginationState );
          eventCount += 1;
        } else {
          if ( eventCount == eventTrigger ) {
            eventCount = 0;
          } else {
            eventCount += 1;
          }
        }
      }

      //progress = state.createStateProgress(progress);
      final ProcessState nextState = state.advance();
      state.setErrorHandler( IgnoreEverythingReportErrorHandler.INSTANCE );
      state = nextState.commit();

      if ( errorHandler.isErrorOccured() == true ) {
        final List childExceptions = Arrays.asList( errorHandler.getErrors() );
        errorHandler.clearErrors();
        if ( failOnError ) {
          throw new ReportEventException( "Failed to dispatch an event.", childExceptions );
        } else {
          final ReportEventException exception =
            new ReportEventException( "Failed to dispatch an event.", childExceptions );
          AbstractReportProcessor.logger.error( "Failed to dispatch an event.", exception );
        }
      }
    }
    return state;
  }

  protected abstract OutputFunction createLayoutManager();


  protected void prepareReportProcessing() throws ReportProcessingException {
    if ( stateList != null ) {
      // is already paginated.
      return;
    }

    PerformanceLoggingStopWatch sw = getPerformanceMonitorContext().createStopWatch( PerformanceTags.REPORT_PREPARE );
    try {
      sw.start();
      // every report processing starts with an StartState.
      final DefaultProcessingContext processingContext = createProcessingContext();
      final MasterReport report = getReport();
      final OutputFunction lm = createLayoutManager();

      final ProcessState startState = new ProcessState();
      try {
        startState.initializeForMasterReport
          ( report, processingContext, (OutputFunction) lm.getInstance() );
      } finally {
        activeDataFactory = startState.getProcessHandle();
      }
      ProcessState state = startState;
      final int maxRows = startState.getNumberOfRows();

      // the report processing can be split into 2 separate processes.
      // The first is the ReportPreparation; all function values are resolved and
      // a dummy run is done to calculate the final layout. This dummy run is
      // also necessary to resolve functions which use or depend on the PageCount.

      // the second process is the printing of the report, this is done in the
      // processReport() method.
      processingContext.setPrepareRun( true );

      // now process all function levels.
      // there is at least one level defined, as we added the PageLayouter
      // to the report.
      // the levels are defined from +inf to 0
      // we don't draw and we do not collect states in a StateList yet
      final int[] levels;
      int index;
      if ( state.isStructuralPreprocessingNeeded() ) {
        state = performStructuralPreprocessing( state, processingContext );
        levels = state.getRequiredRuntimeLevels();
        index = 1;
      } else {
        levels = state.getRequiredRuntimeLevels();
        index = 0;
      }

      if ( levels.length == 0 ) {
        throw new IllegalStateException( "Assertation Failed: No functions defined, invalid implementation." );
      }
      final PerformanceLoggingStopWatch preDataSw =
        getPerformanceMonitorContext().createStopWatch( PerformanceTags.REPORT_PREPARE_DATA );
      try {
        preDataSw.start();

        processingContext.setProgressLevelCount( levels.length );
        int level = levels[ index ];
        // outer loop: process all function levels
        boolean hasNext;
        do {
          processingContext.setProcessingLevel( level );
          processingContext.setProgressLevel( index );

          // if the current level is the output-level, then save the report state.
          // The state is used later to restart the report processing.
          if ( level == LayoutProcess.LEVEL_PAGINATE ) {
            if ( isFullStreamingProcessor() ) {
              stateList = new FastPageStateList( this );
            } else {
              stateList = new DefaultPageStateList( this );
            }
            physicalMapping = new IntList( 40 );
            logicalMapping = new IntList( 20 );
            AbstractReportProcessor.logger.debug( "Pagination started .." );
            state = processPaginationLevel( state, stateList, maxRows );
          } else {
            preDataSw.start();
            state = processPrepareLevels( state, maxRows );
            preDataSw.stop( true );
          }

          // if there is an other level to process, then use the finish state to
          // create a new start state, which will continue the report processing on
          // the next higher level.
          hasNext = ( index < ( levels.length - 1 ) );
          if ( hasNext ) {
            index += 1;
            level = levels[ index ];
            processingContext.setProcessingLevel( level );
            processingContext.setProgressLevel( index );
            if ( state.isFinish() ) {
              state = state.restart();
            } else {
              throw new IllegalStateException(
                "Repaginate did not produce an finish state" );
            }
          }
        }
        while ( hasNext == true );
      } finally {
        preDataSw.close();
      }
      // finally return the saved page states.
      processingContext.setPrepareRun( false );
    } finally {
      sw.close();
    }
  }

  public void setFullStreamingProcessor( final boolean fullStreamingProcessor ) {
    this.fullStreamingProcessor = fullStreamingProcessor;
  }

  public boolean isFullStreamingProcessor() {
    return fullStreamingProcessor;
  }

  private ProcessState performStructuralPreprocessing( final ProcessState startState,
                                                       final DefaultProcessingContext processingContext )
    throws ReportProcessingException {
    PerformanceLoggingStopWatch sw =
      getPerformanceMonitorContext().createStopWatch( PerformanceTags.REPORT_PREPARE_CROSSTAB );
    try {
      sw.start();
      processingContext.setProcessingLevel( LayoutProcess.LEVEL_STRUCTURAL_PREPROCESSING );
      processingContext.setProgressLevel( -1 );

      final int maxRows = startState.getNumberOfRows();
      ProcessState state = processPrepareLevels( startState, maxRows );
      if ( state.isFinish() ) {
        state = state.restart();
      } else {
        throw new IllegalStateException( "Repaginate did not produce an finish state" );
      }
      return state;
    } finally {
      sw.close();
    }
  }

  class ProcessPaginationLevelLoopContext {
    
    final ProcessState startState;
    final PageStateList pageStates;
    final int maxRows;
    
    final boolean failOnError;
    final ReportProcessingErrorHandler errorHandler;
    final DefaultLayoutPagebreakHandler pagebreakHandler;

    final ProcessState initialReportState;
    final PageState initialPageState;
    final ReportProgressEvent repaginationState;
    final int eventTrigger;
    ProcessState state;
    final OutputProcessorMetaData metaData;

    int pageEventCount;
    ProcessState fallBackState;
    ProcessState globalState;

    ReportStateKey rollbackPageState;
    boolean isInRollBackMode;

    int eventCount;
    int lastRow;
    int loopI = -1;
    public ProcessPaginationLevelLoopContext( //
        ProcessState startState, //
        PageStateList pageStates, //
        int maxRows, //
        boolean failOnError, //
        ReportProcessingErrorHandler errorHandler, //
        DefaultLayoutPagebreakHandler pagebreakHandler, //
        ProcessState initialReportState, //
        PageState initialPageState, //
        ReportProgressEvent repaginationState, //
        int eventTrigger, //
        ProcessState state, //
        OutputProcessorMetaData metaData, //
        int pageEventCount, //
        ProcessState fallBackState, //
        ProcessState globalState, //
        ReportStateKey rollbackPageState, //
        boolean isInRollBackMode, //
        int eventCount, //
        int lastRow, //
        int loopI //
    ) {
      super();
      this.startState = startState;
      this.pageStates = pageStates;
      this.maxRows = maxRows;
      this.failOnError = failOnError;
      this.errorHandler = errorHandler;
      this.pagebreakHandler = pagebreakHandler;
      this.initialReportState = initialReportState;
      this.initialPageState = initialPageState;
      this.repaginationState = repaginationState;
      this.eventTrigger = eventTrigger;
      this.state = state;
      this.metaData = metaData;
      this.pageEventCount = pageEventCount;
      this.fallBackState = fallBackState;
      this.globalState = globalState;
      this.rollbackPageState = rollbackPageState;
      this.isInRollBackMode = isInRollBackMode;
      this.eventCount = eventCount;
      this.lastRow = lastRow;
      this.loopI = loopI;
    }
    
  }
  /**
   * Processes the print level for the current report. This function will fill the report state list while performing
   * the repagination.
   *
   * @param ___startState the start state for the print level.
   * @param ___pageStates the list of report states that should receive the created page states.
   * @param ___maxRows    the number of rows in the report (used to estaminate the current progress).
   * @return the finish state for the report.
   * @throws ReportProcessingException if there was a problem processing the report.
   */
  private ProcessState processPaginationLevel( final ProcessState ___startState,
                                               final PageStateList ___pageStates,
                                               final int ___maxRows )
    throws ReportProcessingException {
    Prd5547.inst.beginProcessPaginationLevel();
    PerformanceLoggingStopWatch sw = getPerformanceMonitorContext().createStopWatch( PerformanceTags.REPORT_PAGINATE );
    try {
      sw.start();
      final boolean ___failOnError = isStrictErrorHandling( getReport().getReportConfiguration() );
      final ReportProcessingErrorHandler ___errorHandler = new CollectingReportErrorHandler();
      final DefaultLayoutPagebreakHandler ___pagebreakHandler = new DefaultLayoutPagebreakHandler();

      final ProcessState ___initialReportState = ___startState.deriveForStorage();
      final PageState ___initialPageState = new PageState( ___initialReportState, outputProcessor.getPageCursor() );
      ___pageStates.add( ___initialPageState );

      final ReportProgressEvent ___repaginationState = new ReportProgressEvent( this );

      // inner loop: process the complete report, calculate the function values
      // for the current level. Higher level functions are not available in the
      // dataRow.
      final int ___eventTrigger;
      if ( ___maxRows <= 0 ) {
        ___eventTrigger = Math.max( ___maxRows / MAX_EVENTS_PER_RUN, MIN_ROWS_PER_EVENT );
      } else {
        ___eventTrigger = Math.min( ___maxRows, Math.max( ___maxRows / MAX_EVENTS_PER_RUN, MIN_ROWS_PER_EVENT ) );
      }

      ProcessState ___state = ___startState.deriveForStorage();
      DR.printStackTrace( new Throwable(), "- PPL.00 state = startState.deriveForStorage();" + ___state);
      ___state.setErrorHandler( ___errorHandler );
      validate( ___state );

      final OutputProcessorMetaData ___metaData =
        ___state.getFlowController().getReportContext().getOutputProcessorMetaData();
      pagebreaksSupported = ___metaData.isFeatureSupported( OutputProcessorFeature.PAGEBREAKS );

      int ___pageEventCount = 0;
      // First and last derive of a page must be a storage derivate - this clones everything and does
      // not rely on the more complicated transactional layouting ..
      ProcessState ___fallBackState = pagebreaksSupported ? ___state.deriveForPagebreak() : null;
      ProcessState ___globalState = pagebreaksSupported ? ___state.deriveForStorage() : null;

      ReportStateKey ___rollbackPageState = null;
      boolean ___isInRollBackMode = false;

      int ___eventCount = 0;
      int ___lastRow = -1;
      int ___loopI = 0;
      ProcessPaginationLevelLoopContext ctx = new ProcessPaginationLevelLoopContext(
          ___startState, //
          ___pageStates, //
          ___maxRows, //
          ___failOnError, //
          ___errorHandler, //
          ___pagebreakHandler, //
          ___initialReportState, //
          ___initialPageState, //
          ___repaginationState, //
          ___eventTrigger, //
          ___state, //
          ___metaData, //
          ___pageEventCount, //
          ___fallBackState, //
          ___globalState, //
          ___rollbackPageState, //
          ___isInRollBackMode, //
          ___eventCount, //
          ___lastRow, //
          ___loopI //
          );
      while ( !ctx.state.isFinish() ) {
        {
          ctx.loopI++;
        }
        DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
            "- PPL.05 loop=" + ( ctx.loopI ) + //
                " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + " begin; " + ctx.state );
        int logPageCount = outputProcessor.getLogicalPageCount();
        int physPageCount = outputProcessor.getPhysicalPageCount();

        checkInterrupted();
        if ( ctx.lastRow != ctx.state.getCurrentRow() ) {
          ctx.lastRow = ctx.state.getCurrentRow();
          if ( ctx.eventCount == 0 ) {
            if ( isPagebreaksSupported() && ctx.fallBackState != null ) {
              ctx.repaginationState
                .reuse( ReportProgressEvent.PAGINATING, ctx.fallBackState, calculatePageCount( ctx.fallBackState ) );
            } else {
              ctx.repaginationState.reuse( ReportProgressEvent.PAGINATING, ctx.state, calculatePageCount( ctx.state ) );
            }
            fireStateUpdate( ctx.repaginationState );
            ctx.eventCount += 1;
          } else {
            if ( ctx.eventCount == ctx.eventTrigger ) {
              ctx.eventCount = 0;
            } else {
              ctx.eventCount += 1;
            }
          }
        }

        // Do not try to layout on a artificial state. Those states are not valid on
        // generating page events and cannot be trusted.
        ProcessState realFallbackState = ctx.fallBackState;
        final ProcessState restoreState;
        if ( pagebreaksSupported && ctx.state.isArtifcialState() == false ) {
          restoreState = ctx.fallBackState;
          if ( ctx.isInRollBackMode == false ) {
            if ( ctx.pageEventCount >= AbstractReportProcessor.COMMIT_RATE ) {
              final OutputFunction outputFunction = ctx.state.getLayoutProcess().getOutputFunction();
              if ( outputFunction.createRollbackInformation() ) {
                realFallbackState = ctx.state.deriveForPagebreak();
                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                  logger.debug( "Paginate: Try to generate new fallback state after commit count reached: " + ctx.state
                    .getProcessKey() );
                }
                validate( ctx.state );
              } else {
                realFallbackState = null;
              }
            }
          }
        } else {
          restoreState = null;
        }

        final ProcessState nextState = ctx.state.advance();
        ctx.state.setErrorHandler( IgnoreEverythingReportErrorHandler.INSTANCE );
        ctx.state = nextState;
        DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
            "- PPL.10 loop=" + ( ctx.loopI ) + //
                " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); afterAdvance " + ctx.state );
        validate( ctx.state );

        final ReportStateKey nextStateKey = ctx.state.getProcessKey();

        if ( ctx.errorHandler.isErrorOccured() == true ) {
          final List childExceptions = Arrays.asList( ctx.errorHandler.getErrors() );
          ctx.errorHandler.clearErrors();
          final ReportEventException exception =
            new ReportEventException( "Failed to dispatch an event.", childExceptions );
          if ( ctx.failOnError ) {
            throw exception;
          } else {
            AbstractReportProcessor.logger.error( "Failed to dispatch an event.", exception );
          }
        }

        if ( ctx.state.isArtifcialState() ) {
          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
            logger.debug( "Paginate: Silent commit as we are in an artificial state: " + ctx.state.getProcessKey() );
          }
          ctx.state = ctx.state.commit();
          DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
              "- PPL.10 loop=" + ( ctx.loopI ) + //
                  " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                  " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); afterStateCommit " + ctx.state );
          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
            logger.debug( "Paginate: Post Silent commit as we are in an artificial state: " + ctx.state.getProcessKey() );
          }
          continue;
        }

        final OutputFunction outputFunction = ctx.state.getLayoutProcess().getOutputFunction();
        if ( outputFunction instanceof DefaultOutputFunction == false ) {
          ctx.state = ctx.state.commit();
          DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
              "- PPL.30 state = state.commit(); " + ctx.state );
          if ( ctx.state.isFinish() && ctx.pageStates.size() == 1 ) {
            physicalMapping.add( 0 );
            logicalMapping.add( 0 );
          }
          continue;
        }

        final DefaultOutputFunction lm = (DefaultOutputFunction) outputFunction;
        final Renderer renderer = lm.getRenderer();
        renderer.setStateKey( ctx.state.getProcessKey() );
        ctx.pagebreakHandler.setReportState( ctx.state );

        boolean assertExpectPagebreak = false;
        if ( ctx.isInRollBackMode ) {
          if ( nextStateKey.equals( ctx.rollbackPageState ) ) {
            // reached the border case. We have to insert a manual pagebreak here or at least
            // we have to force the renderer to end the page right now.
            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
              AbstractReportProcessor.logger.debug(
                "Paginate: Found real pagebreak position. This might be the last state we process: "
                  + ctx.rollbackPageState );
              AbstractReportProcessor.logger.debug(
                "Paginate:   (Current state process key)           : " + ctx.state.getProcessKey() );
              AbstractReportProcessor.logger.debug(
                "Paginate:   (Handler)                             : " + ctx.state.getAdvanceHandler().getClass()
                  .getName() );
            }
            assertExpectPagebreak = true;
            DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
                "- PPL.35. loop=" + ( ctx.loopI ) + //
                    " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                    " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); beforePB " + ctx.state );
            renderer.addPagebreak();
            DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
                "- PPL.36. loop=" + ( ctx.loopI ) + //
                    " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                    " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); afterPB" + ctx.state );
          }
        }

        DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
            "- PPL.37. loop=" + ( ctx.loopI ) + //
                " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); beforeV " + ctx.state );
        final Renderer.LayoutResult pagebreakEncountered = renderer.validatePages();
        DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
            "- PPL.37. loop=" + ( ctx.loopI ) + //
                " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); afterV " + ctx.state );
        if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
          AbstractReportProcessor.logger.debug( "Paginate: Validate Page returned " + pagebreakEncountered );
          if ( assertExpectPagebreak == true && pagebreakEncountered != Renderer.LayoutResult.LAYOUT_PAGEBREAK ) {
            AbstractReportProcessor.logger.debug( "Paginate: Missed the pagebreak. This smells fishy!" );
          }
        }

        if ( pagebreakEncountered != Renderer.LayoutResult.LAYOUT_UNVALIDATABLE ) {//!!!!!!!!!!!!!!!
          if ( pagebreaksSupported && ctx.state.isArtifcialState() == false ) {
            if ( ctx.isInRollBackMode == false ) {
              if ( ctx.pageEventCount >= AbstractReportProcessor.COMMIT_RATE ) {
                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                  logger.debug(
                    "Paginate: Try to apply new fallback state after commit count reached: " + ctx.state.getProcessKey() );
                  logger.debug( "Paginate:        : " + renderer.getLastStateKey() );
                }

                ctx.fallBackState = realFallbackState;
                ctx.pageEventCount = 0;
              } else {
                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                  logger.debug( "Paginate: Increase counter: " + ctx.state.getProcessKey() );
                }
                ctx.pageEventCount += 1;
              }
            }
          }
        } else if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
          if ( pagebreaksSupported && ctx.state.isArtifcialState() == false ) {
            if ( ctx.isInRollBackMode == false ) {
              logger.debug( "Paginate: SKIP : " + ctx.state.getProcessKey() );
            }
          }
        }

        if ( pagebreakEncountered == Renderer.LayoutResult.LAYOUT_PAGEBREAK ) {
          final boolean onGoingPageBreak;

          //          renderer.print();
          final ReportStateKey lastVisibleStateKey = renderer.getLastStateKey();
          if ( isPagebreaksSupported() &&
            ctx.isInRollBackMode == false &&
            lastVisibleStateKey != null &&
            renderer.isOpen() ) {
            if ( lastVisibleStateKey.equals( nextStateKey ) == false &&
              lastVisibleStateKey.getSequenceCounter() > ctx.globalState.getProcessKey().getSequenceCounter() ) {
              // Roll back to the last known to be good position and process the states up to, but not
              // including the current state. This way, we can fire the page-events *before* this band
              // gets printed.
              ctx.rollbackPageState = lastVisibleStateKey;

              final ReportStateKey restoreStateProcessKey = restoreState.getProcessKey();
              if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                AbstractReportProcessor.logger.debug(
                  "Paginate: Encountered bad break, need to roll-back: " + ctx.rollbackPageState );
                AbstractReportProcessor.logger.debug(
                  "Paginate:    Next StateKey                        : " + ctx.state.getProcessKey() );
                AbstractReportProcessor.logger.debug(
                  "Paginate:    Restored Key                         : " + restoreStateProcessKey );
                AbstractReportProcessor.logger.debug(
                  "Paginate:    Position in event chain              : " + restoreState.getSequenceCounter() );
              }
              if ( lastVisibleStateKey.getSequenceCounter() < restoreStateProcessKey.getSequenceCounter() ) {
                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                  AbstractReportProcessor.logger.debug(
                    "Paginate: Fall back to start of page              : " + ctx.globalState.getProcessKey() );
                }
                if ( lastVisibleStateKey.getSequenceCounter() <= ctx.globalState.getProcessKey().getSequenceCounter() ) {
                  throw new ReportProcessingException(
                    "Paginate: Error, fallback position is after last visible state." );
                }
                ctx.state = ctx.globalState.deriveForStorage();
                DR.printStackTrace( new Throwable(), "- PPL.40 state = globalState.deriveForStorage(); " + ctx.state);
              } else {
                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                  AbstractReportProcessor.logger.debug(
                    "Paginate: Fall back to save-state                 : " + restoreStateProcessKey );
                }
                ctx.state = restoreState.deriveForPagebreak();
                DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
                    "- PPL.50. loop=" + ( ctx.loopI ) + //
                        " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                        " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); derive " + ctx.state );
              }

              final DefaultOutputFunction rollbackOutputFunction =
                (DefaultOutputFunction) ctx.state.getLayoutProcess().getOutputFunction();
              final Renderer rollbackRenderer = rollbackOutputFunction.getRenderer();
              rollbackRenderer.rollback();

              validate( ctx.state );

              ctx.isInRollBackMode = true;
              ctx.fallBackState = null; // there is no way we can fall-back inside a roll-back ..
              DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
                  "- PPL.52. loop=" + ( ctx.loopI ) + //
                      " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                      " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); resetRB.pBreak " + ctx.state );
              continue;
            }

            // when (lastVisibleStateKey.equals(nextStateKey))

            // The current state printed content partially on the now finished page and there is more
            // content on the currently open page. This is a in-between pagebreak, we invoke a pagebreak
            // after this state has been processed.
            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
              AbstractReportProcessor.logger.debug( "Paginate: Encountered on-going break " + lastVisibleStateKey );
            }

            onGoingPageBreak = true;
            ctx.rollbackPageState = null;
          } else {
            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
              if ( ctx.isInRollBackMode ) {
                AbstractReportProcessor.logger.debug( "Paginate: Encountered a roll-back break: " + ctx.isInRollBackMode );
                if ( assertExpectPagebreak == false ) {
                  AbstractReportProcessor.logger.debug( "Paginate: next state:     " + nextStateKey );
                  if ( nextStateKey.equals( ctx.rollbackPageState ) == false ) {
                    AbstractReportProcessor.logger.debug( "Paginate: rollback state: " + ctx.rollbackPageState );
                  }
                }
              } else {
                AbstractReportProcessor.logger.debug( "Paginate: Encountered a good break: " + ctx.isInRollBackMode );
              }
              AbstractReportProcessor.logger.debug
                ( "Paginate:                                              : " + ctx.state.getProcessKey() );
            }
            ctx.isInRollBackMode = false;
            ctx.rollbackPageState = null;
            onGoingPageBreak = false;
            DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
                "- PPL.52. loop=" + ( ctx.loopI ) + //
                    " sc=" + ctx.state.getProcessKey().getSequenceCounter() + //
                    " rb=" + ( ctx.isInRollBackMode ? "T" : "_" ) + "); resetRB.noPBreak " + ctx.state );
          }

          if ( isPagebreaksSupported() == false ) {
            // The commit causes all closed-nodes to become finishable. This allows the process-page
            // and the incremental-update methods to remove the nodes. For non-streaming targets (where
            // pagebreaks are possible) the commit state is managed manually
            renderer.applyAutoCommit();
          }

          if ( renderer.processPage( ctx.pagebreakHandler, ctx.state.getProcessKey(), true ) == false ) {
            throw new IllegalStateException
              ( "This cannot be. If the validation said we get a new page, how can we now get lost here" );
          }

          if ( isPagebreaksSupported() ) {
            if ( renderer.isPendingPageHack() && renderer.isCurrentPageEmpty() == false
              && renderer.isPageStartPending() == false ) {
              if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                logger.debug(
                  "Paginate: Delaying next event to allow pending pages to be processed: " + ctx.state.getProcessKey() );
              }
              ctx.state = PendingPagesHandler.create( ctx.state );
              DR.printStackTrace( new Throwable(), "- PPL.60 state = PendingPagesHandler.create(state); " + ctx.state);
            } else {
              if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
                logger
                  .debug( "Paginate: Adding RestartOnNewPageHandler to open Page in time: " + ctx.state.getProcessKey() );
              }
              ctx.state = RestartOnNewPageHandler.create( ctx.state.commit() );
              DR.printStackTrace( new Throwable(), "- PPL.70 state = RestartOnNewPageHandler.create(state.commit()); " + ctx.state);
            }
          } else {
            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
              logger.debug(
                "Paginate: Commit on page-break: " + ctx.state.getProcessKey() + " " + ctx.state.getAdvanceHandler().getClass()
                  .getName() );
            }
            ctx.state = ctx.state.commit();
            DR.printStackTrace( new Throwable(), "- PPL.80 state = state.commit(); " + ctx.state);
          }

          if ( onGoingPageBreak ) {
            renderer.setStateKey( ctx.state.getProcessKey() );
            renderer.addProgressBox();
          }

          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
            logger.debug( "Paginate: Post Commit: " + ctx.state.getProcessKey() + " " + ctx.state.getAdvanceHandler().getClass()
              .getName() );
          }

          // can continue safely ..
          final int newLogPageCount = outputProcessor.getLogicalPageCount();
          final int newPhysPageCount = outputProcessor.getPhysicalPageCount();

          final int result = ctx.pageStates.size() - 1;
          for (; physPageCount < newPhysPageCount; physPageCount++ ) {
            physicalMapping.add( result );
          }

          for (; logPageCount < newLogPageCount; logPageCount++ ) {
            logicalMapping.add( result );
          }

          if ( ctx.state.isFinish() == false ) {
            // A pagebreak has occured ...
            // We add all but the last state ..
            final PageState pageState = new PageState( ctx.state, outputProcessor.getPageCursor() );
            ctx.pageStates.add( pageState );
          }

          if ( isPagebreaksSupported() ) {
            ctx.fallBackState = ctx.state.deriveForPagebreak();
            ctx.globalState = ctx.state.deriveForStorage();
            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
              logger.debug( "Paginate: Generating new fallback state after pagebreak found: " + ctx.state.getProcessKey() );
            }
            ctx.pageEventCount = 0;
            ctx.eventCount = 0;
          }
        } else {
          if ( isPagebreaksSupported() == false ) {//TODO this "if" is useless
            renderer.applyAutoCommit();
          }

          // PageEventCount is zero on streaming exports and zero after a new rollback event is created.
          if ( ctx.pageEventCount == 0 && ctx.isInRollBackMode == false &&
            pagebreakEncountered == Renderer.LayoutResult.LAYOUT_NO_PAGEBREAK ) {
            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
              logger.debug( "Paginate: Perform incremental update: " + ctx.state.getProcessKey() );
            }
            renderer.processIncrementalUpdate( false );
            //fallBackState = state;
          }
          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
            logger.debug(
              "Paginate: Commit: " + ctx.state.getProcessKey() + " " + ctx.state.getAdvanceHandler().getClass().getName() );
          }
          ctx.state = ctx.state.commit();
          DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
              "- PPL.90. loop=" + ( ctx.loopI) + // 
                " sc=" + ctx.state.getProcessKey().getSequenceCounter()+//
                " rb="+ (ctx.isInRollBackMode?"T":"_")+ "); afterStateCommit " + ctx.state);
          //fallBackState = state.deriveForPagebreak();
          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
            logger.debug( "Paginate: Post Commit: " + ctx.state.getProcessKey() + " " + ctx.state.getAdvanceHandler().getClass()
              .getName() );
          }

          //          printLayoutStateToFile(state, false, isInRollBackMode);

          if ( pagebreaksSupported && ctx.fallBackState != restoreState ) {
            final DefaultOutputFunction commitableOutputFunction =
              (DefaultOutputFunction) ctx.state.getLayoutProcess().getOutputFunction();
            final Renderer commitableRenderer = commitableOutputFunction.getRenderer();
            commitableRenderer.applyRollbackInformation();
            //fallBackState = state.deriveForPagebreak();
            DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
                "- PPL.95. loop=" + ( ctx.loopI) + // 
                  " sc=" + ctx.state.getProcessKey().getSequenceCounter()+//
                  " rb="+ (ctx.isInRollBackMode?"T":"_")+ "); afterApplyRBI " + ctx.state);
          }
        }

      }
      
      //try {
      //  BSh.create().set("filter", true).set( "node",  //
      //      (((org.pentaho.reporting.engine.classic.core.layout.AbstractRenderer)((org.pentaho.reporting.engine.classic.core.layout.output.DefaultOutputFunction)state.getLayoutProcess().getOutputFunction()).getRenderer()).getRenderModelBuilder().getPageBox()) //
      //  ).getInterpreter().eval( new AutocloseReader( new java.io.FileReader( "D:\\PENT\\work\\PRD-5547\\display-node.bsh" ) ) );
      //} catch ( Exception e ) {
      //  e.printStackTrace();
      //}
      /*state-node*/
      DR.printTraceAndNode( new Throwable(), getStatePageBox( ctx ), //
          "- PPL.99 processPaginationLevel result "+ // 
            " sc=" + ctx.state.getProcessKey().getSequenceCounter()+//
            " rb="+ (ctx.isInRollBackMode?"T":"_")+ "); " + ctx.state);
      return ctx.initialReportState;
    } catch ( ContentProcessingException e ) {
      throw new ReportProcessingException( "Content-Processing failed.", e );
    } finally {
      Prd5547.inst.endProcessPaginationLevel();
      sw.close();
    }
  }

  private LogicalPageBox getStatePageBox( ProcessPaginationLevelLoopContext ctx ) {
    return ((AbstractRenderer)((DefaultOutputFunction)ctx.state.getLayoutProcess().getOutputFunction()).getRenderer()).getRenderModelBuilder().getPageBox();
  }

  @SuppressWarnings( "UnusedDeclaration" )
  private void printLayoutStateToFile( final ProcessState state,
                                       final boolean print,
                                       final boolean inRollBackMode ) {
    final DefaultOutputFunction of =
      (DefaultOutputFunction) state.getLayoutProcess().getOutputFunction();
    final AbstractRenderer ar = (AbstractRenderer) of.getRenderer();
    ar.printLayoutStateToFile( state, print, inRollBackMode );
  }


  protected int calculatePageCount( final ProcessState state ) {
    final OutputFunction outputFunction = state.getLayoutProcess().getOutputFunction();
    if ( outputFunction instanceof DefaultOutputFunction == false ) {
      return 0;
    }

    final DefaultOutputFunction lm = (DefaultOutputFunction) outputFunction;
    final Renderer renderer = lm.getRenderer();
    return renderer.getPageCount() + 1;
  }

  private void validate( final ProcessState state ) {
    if ( paranoidChecks ) {
      final OutputFunction outputFunction = state.getLayoutProcess().getOutputFunction();
      if ( outputFunction instanceof DefaultOutputFunction == false ) {
        return;
      }

      final DefaultOutputFunction of = (DefaultOutputFunction) outputFunction;

      final Renderer renderer = of.getRenderer();
      if ( renderer instanceof AbstractRenderer ) {
        final AbstractRenderer r = (AbstractRenderer) renderer;
        r.performParanoidModelCheck();
      }
    }
  }

  public boolean isPaginated() {
    return stateList != null;
  }

  protected PageState getLogicalPageState( final int page ) {
    final int index = logicalMapping.get( page );
    final PageState pageState = stateList.get( index );
    if ( pageState == null ) {
      throw new IndexOutOfBoundsException(
        "The logical mapping between page " + page + " and index " + index + " is invalid." );
    }
    return pageState;
  }

  protected PageState getPhysicalPageState( final int page ) {
    final int index = physicalMapping.get( page );
    final PageState pageState = stateList.get( index );
    if ( pageState == null ) {
      throw new IndexOutOfBoundsException(
        "The physical mapping between page " + page + " and index " + index + " is invalid." );
    }
    return pageState;
  }


  /**
   * Checks whether report processing should be aborted when an exception occurs.
   *
   * @param config the configuration.
   * @return if strict error handling is enabled.
   */
  protected static boolean isStrictErrorHandling( final Configuration config ) {
    final String strictError = config.getConfigProperty( ClassicEngineCoreModule.STRICT_ERROR_HANDLING_KEY );
    return "true".equals( strictError );
  }

  public PageState processPage( final PageState pageState,
                                final boolean performOutput )
    throws ReportProcessingException {
    Prd5547.inst.beginProcessPage();
    
    if ( pageState == null ) {
      throw new NullPointerException( "PageState must not be null." );
    }
    final boolean failOnError = isStrictErrorHandling( getReport().getReportConfiguration() );
    final ReportProcessingErrorHandler errorHandler = new CollectingReportErrorHandler();
//    if ( SHOW_ROLLBACKS ) {
//      AbstractReportProcessor.logger.debug(
//        "Process Page: Starting with : " + pageState.getReportState().getProcessKey() );
//    }

    try {
      final ProcessState startState = pageState.getReportState();
      outputProcessor.setPageCursor( pageState.getPageCursor() );
      final int maxRows = startState.getNumberOfRows();
      final ReportProgressEvent repaginationState = new ReportProgressEvent( this );
      final DefaultLayoutPagebreakHandler pagebreakHandler = new DefaultLayoutPagebreakHandler();
      // inner loop: process the complete report, calculate the function values
      // for the current level. Higher level functions are not available in the
      // dataRow.
      final int eventTrigger;
      if ( maxRows <= 0 ) {
        eventTrigger = Math.max( maxRows / MAX_EVENTS_PER_RUN, MIN_ROWS_PER_EVENT );
      } else {
        eventTrigger = Math.min( maxRows, Math.max( maxRows / MAX_EVENTS_PER_RUN, MIN_ROWS_PER_EVENT ) );
      }

      final boolean pagebreaksSupported = isPagebreaksSupported();

      ReportStateKey rollbackPageState = null;

      ProcessState state = startState.deriveForStorage();
      DR.printStackTrace( new Throwable(), "- PP.00 state = startState.deriveForStorage(); " + state);
      ProcessState fallBackState = pagebreaksSupported ? state.deriveForPagebreak() : null;
      final ProcessState globalState = pagebreaksSupported ? state.deriveForStorage() : null;
      state.setErrorHandler( errorHandler );

      boolean isInRollBackMode = false;
      int lastRow = -1;
      int eventCount = 0;
      int pageEventCount = 0;
      int iterI =0;
      while ( !state.isFinish() ) {
        //boolean rollbackProcessed = false;
        iterI++;
        DR.printStackTrace(new Throwable(), "---iterI " + iterI + " " + state);
        try {
          DR.printText( (String)BSh.create().set("filter", true).set( "node", (((org.pentaho.reporting.engine.classic.core.layout.AbstractRenderer)((org.pentaho.reporting.engine.classic.core.layout.output.DefaultOutputFunction)state.getLayoutProcess().getOutputFunction()).getRenderer()).getRenderModelBuilder().getPageBox()) ).getInterpreter().eval( new AutocloseReader( new java.io.FileReader( "D:\\\\PENT\\\\work\\\\PRD-5547\\\\display-node.bsh" ) ) )/*state-node*/
              , "---iterI " + iterI + " node", true );
        } catch ( Exception e ) {
          e.printStackTrace();
        }
        checkInterrupted();
        if ( lastRow != state.getCurrentRow() ) {
          lastRow = state.getCurrentRow();
          if ( eventCount == 0 ) {
            repaginationState.reuse( ReportProgressEvent.GENERATING_CONTENT, state, calculatePageCount( state ) );
            fireStateUpdate( repaginationState );
            eventCount += 1;
          } else {
            if ( eventCount == eventTrigger ) {
              eventCount = 0;
            } else {
              eventCount += 1;
            }
          }
        }

        ProcessState realFallbackState = fallBackState;
        final ProcessState restoreState;
        if ( pagebreaksSupported && state.isArtifcialState() == false ) {
          restoreState = fallBackState;
          if ( isInRollBackMode == false ) {
            if ( pageEventCount >= AbstractReportProcessor.COMMIT_RATE ) {
              final OutputFunction outputFunction = state.getLayoutProcess().getOutputFunction();
              if ( outputFunction.createRollbackInformation() ) {
//                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                  logger.debug(
//                    "Print: Try to generate new fallback state after commit count reached: " + state.getProcessKey() );
//                }
                realFallbackState = state.deriveForPagebreak();
              } else {
                realFallbackState = null;
              }
            }
          }
        } else {
          restoreState = null;
        }

        final ProcessState nextState = state.advance();
        state.setErrorHandler( IgnoreEverythingReportErrorHandler.INSTANCE );
        state = nextState;

        final ReportStateKey nextStateKey = state.getProcessKey();

        if ( errorHandler.isErrorOccured() == true ) {
          final List childExceptions = Arrays.asList( errorHandler.getErrors() );
          errorHandler.clearErrors();
          if ( failOnError ) {
            throw new ReportEventException( "Failed to dispatch an event.", childExceptions );
          } else {
            final ReportEventException exception =
              new ReportEventException( "Failed to dispatch an event.", childExceptions );
            AbstractReportProcessor.logger.error( "Failed to dispatch an event.", exception );
          }
        }

        if ( state.isArtifcialState() ) {
//          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//            logger.debug( "Print: Silent commit as we are in an artificial state: " + state.getProcessKey() );
//          }
          state = state.commit();
//          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//            logger.debug( "Print: Post Silent commit as we are in an artificial state: " + state.getProcessKey() );
//          }
          continue;
        }

        final OutputFunction outputFunction = state.getLayoutProcess().getOutputFunction();
        if ( outputFunction instanceof DefaultOutputFunction == false ) {
//          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//            logger.debug( "Print: Silent commit as we are have no access to the renderer: " + state.getProcessKey() );
//          }
          state = state.commit();
          continue;
        }

        final DefaultOutputFunction lm = (DefaultOutputFunction) outputFunction;
        final Renderer renderer = lm.getRenderer();
        renderer.setStateKey( state.getProcessKey() );
        pagebreakHandler.setReportState( state );

        boolean assertExpectPagebreak = false;
        if ( isInRollBackMode ) {
          if ( nextStateKey.equals( rollbackPageState ) ) {
            // reached the border case. We have to insert a manual pagebreak here or at least
            // we have to force the renderer to end the page right now.
//            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//              AbstractReportProcessor.logger.debug(
//                "Print: Found real pagebreak position. This might be the last state we process: " + rollbackPageState );
//              AbstractReportProcessor.logger.debug(
//                "Print:   (Current state process key)           : " + state.getProcessKey() );
//              AbstractReportProcessor.logger.debug(
//                "Print:   (Handler)                             : " + state.getAdvanceHandler().getClass().getName() );
//            }
            assertExpectPagebreak = true;
            renderer.addPagebreak();
            //rollbackProcessed = true;
          }
        }

        try {
          DR.printText( (String)BSh.create().set("filter", true).set( "node", (((org.pentaho.reporting.engine.classic.core.layout.AbstractRenderer)((org.pentaho.reporting.engine.classic.core.layout.output.DefaultOutputFunction)state.getLayoutProcess().getOutputFunction()).getRenderer()).getRenderModelBuilder().getPageBox()) ).getInterpreter().eval( new AutocloseReader( new java.io.FileReader( "D:\\\\PENT\\\\work\\\\PRD-5547\\\\display-node.bsh" ) ) )/*state-node*/
              , "  ---iterI " + iterI + " node.before validatePages" );
        } catch ( Exception e ) {
          e.printStackTrace();
        }
        final Renderer.LayoutResult pagebreakEncountered = renderer.validatePages();
        try {
          DR.printText( (String)BSh.create().set("filter", true).set( "node", (((org.pentaho.reporting.engine.classic.core.layout.AbstractRenderer)((org.pentaho.reporting.engine.classic.core.layout.output.DefaultOutputFunction)state.getLayoutProcess().getOutputFunction()).getRenderer()).getRenderModelBuilder().getPageBox()) ).getInterpreter().eval( new AutocloseReader( new java.io.FileReader( "D:\\\\PENT\\\\work\\\\PRD-5547\\\\display-node.bsh" ) ) )/*state-node*/
              , "  ---iterI " + iterI + " node.after validatePages" );
        } catch ( Exception e ) {
          e.printStackTrace();
        }
//        if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//          AbstractReportProcessor.logger.debug( "Print: Validate Page returned " + pagebreakEncountered );
//          if ( assertExpectPagebreak == true && pagebreakEncountered != Renderer.LayoutResult.LAYOUT_PAGEBREAK ) {
//            AbstractReportProcessor.logger.debug( "Print: Missed the pagebreak. This smells fishy!" );
//          }
//        }

        if ( pagebreakEncountered != Renderer.LayoutResult.LAYOUT_UNVALIDATABLE ) {
          if ( pagebreaksSupported && state.isArtifcialState() == false ) {
            if ( isInRollBackMode == false ) {
              if ( pageEventCount >= AbstractReportProcessor.COMMIT_RATE ) {
//                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                  logger.debug(
//                    "Print: Try to apply new fallback state after commit count reached: " + state.getProcessKey() );
//                  logger.debug( "Print:        : " + renderer.getLastStateKey() );
//                }
                fallBackState = realFallbackState;
                pageEventCount = 0;
              } else {
//                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                  logger.debug( "Print: Increase counter: " + state.getProcessKey() );
//                }
                pageEventCount += 1;
              }
            }
          }
//        } else if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//          if ( pagebreaksSupported && state.isArtifcialState() == false ) {
//            if ( isInRollBackMode == false ) {
//              logger.debug( "Print: SKIP : " + state.getProcessKey() );
//            }
//          }
        }

        if ( pagebreakEncountered == Renderer.LayoutResult.LAYOUT_PAGEBREAK ) {
          final boolean onGoingPageBreak;

          final ReportStateKey lastVisibleStateKey = renderer.getLastStateKey();
          if ( pagebreaksSupported &&
            isInRollBackMode == false &&
            renderer.isOpen() &&
            lastVisibleStateKey != null ) {
            if ( lastVisibleStateKey.equals( nextStateKey ) == false &&
              lastVisibleStateKey.getSequenceCounter() > globalState.getProcessKey().getSequenceCounter() ) {
              // Roll back to the last known to be good position and process the states up to, but not
              // including the current state. This way, we can fire the page-events *before* this band
              // gets printed.
              rollbackPageState = lastVisibleStateKey;

              final ReportStateKey restoreStateProcessKey = restoreState.getProcessKey();
//              if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                AbstractReportProcessor.logger.debug(
//                  "Print: Encountered bad break, need to roll-back: " + rollbackPageState );
//                AbstractReportProcessor.logger.debug(
//                  "Print:    Next StateKey                        : " + state.getProcessKey() );
//                AbstractReportProcessor.logger.debug(
//                  "Print:    Restored Key                         : " + restoreStateProcessKey );
//                AbstractReportProcessor.logger.debug(
//                  "Print:    Position in event chain              : " + restoreState.getSequenceCounter() );
//              }

              if ( lastVisibleStateKey.getSequenceCounter() < restoreStateProcessKey.getSequenceCounter() ) {
//                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                  AbstractReportProcessor.logger.debug(
//                    "Print: Fall back to start of page              : " + globalState.getProcessKey() );
//                }
                if ( lastVisibleStateKey.getSequenceCounter() <= globalState.getProcessKey().getSequenceCounter() ) {
                  throw new ReportProcessingException( "Print: Error, fallback position is after last visible state." );
                }
                state = globalState.deriveForStorage();
              } else {
//                if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                  AbstractReportProcessor.logger.debug(
//                    "Print: Fall back to save-state                 : " + restoreStateProcessKey );
//                }
                state = restoreState.deriveForPagebreak();
              }

              final DefaultOutputFunction rollbackOutputFunction =
                (DefaultOutputFunction) state.getLayoutProcess().getOutputFunction();
              final Renderer rollbackRenderer = rollbackOutputFunction.getRenderer();
              rollbackRenderer.rollback();

              validate( state );

              isInRollBackMode = true;
              DR.printStackTrace(new Throwable(), "   ---RB-- isInRollBackMode = true");

              continue;
            }

            // The current state printed content partially on the now finished page and there is more
            // content on the currently open page. This is a in-between pagebreak, we invoke a pagebreak
            // after this state has been processed.
//            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//              AbstractReportProcessor.logger.debug( "Print: Encountered on-going break " + lastVisibleStateKey );
//            }

            onGoingPageBreak = true;
            rollbackPageState = null;
          } else {
//            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//              if ( isInRollBackMode ) { //INFO: LOG_ONLY
//                if ( assertExpectPagebreak == false ) {
//                  AbstractReportProcessor.logger.debug( "Print: Encountered a roll-back break: " + isInRollBackMode );
//                  if ( assertExpectPagebreak == false ) {
//                    AbstractReportProcessor.logger.debug( "Print: next state:     " + nextStateKey );
//                    if ( nextStateKey.equals( rollbackPageState ) == false ) {
//                      AbstractReportProcessor.logger.debug( "Print: rollback state: " + rollbackPageState );
//                    }
//                  }
//                }
//              } else {
//                AbstractReportProcessor.logger.debug( "Print: Encountered a good break: " + isInRollBackMode );
//              }
//              AbstractReportProcessor.logger.debug
//                ( "Print:                                              : " + state.getProcessKey() );
//            }
            onGoingPageBreak = false;
          }
          if ( pagebreaksSupported == false ) {
            // The commit causes all closed-nodes to become finishable. This allows the process-page
            // and the incremental-update methods to remove the nodes. For non-streaming targets (where
            // pagebreaks are possible) the commit state is managed manually
            renderer.applyAutoCommit();
          }

          if ( renderer.processPage( pagebreakHandler, state.getProcessKey(), performOutput ) == false ) {
            throw new IllegalStateException( "This must not be." );
          }

          if ( isPagebreaksSupported() ) {
            if ( renderer.isPendingPageHack() &&
              renderer.isCurrentPageEmpty() == false && renderer.isPageStartPending() == false ) {
//              if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                logger.debug(
//                  "Print: Delaying next event to allow pending pages to be processed: " + state.getProcessKey() );
//              }
              state = PendingPagesHandler.create( state );
            } else {
//              if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//                logger.debug( "Print: Adding RestartOnNewPageHandler to open Page in time: " + state.getProcessKey() );
//              }
              state = RestartOnNewPageHandler.create( state.commit() );
            }
          } else {
//            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//              logger.debug(
//                "Print: Commit on page-break: " + state.getProcessKey() + " " + state.getAdvanceHandler().getClass()
//                  .getName() );
//            }
            state = state.commit();
          }

          if ( onGoingPageBreak ) {
            renderer.setStateKey( state.getProcessKey() );
            renderer.addProgressBox();
          }

//          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//            logger.debug(
//              "Print: Post Commit: " + state.getProcessKey() + " " + state.getAdvanceHandler().getClass().getName() );
//          }

//          if ( isPagebreaksSupported() ) {
//            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//              logger.debug( "Print: Generating new fallback state after pagebreak found: " + state.getProcessKey() );
//            }
//          }

          if ( renderer.isOpen() ) {
            // No need to create a copy here. It is part of the contract that the resulting page state must be
            // cloned before it can be used again. The only place where it is used is this method, so we can
            // be pretty sure that this contract is valid.
            return new PageState( state, outputProcessor.getPageCursor() );
          }
        } else {
          if ( pagebreaksSupported == false ) {
            // The commit causes all closed-nodes to become finishable. This allows the process-page
            // and the incremental-update methods to remove the nodes. For non-streaming targets (where
            // pagebreaks are possible) the commit state is managed manually
            renderer.applyAutoCommit();
          }

          if ( pageEventCount == 0 && isInRollBackMode == false &&
            pagebreakEncountered == Renderer.LayoutResult.LAYOUT_NO_PAGEBREAK ) {
//            if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//              logger.debug( "Print: Perform incremental update: " + state.getProcessKey() );
//            }
            renderer.processIncrementalUpdate( performOutput );
          }
//          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//            logger.debug(
//              "Print: Commit: " + state.getProcessKey() + " " + state.getAdvanceHandler().getClass().getName() );
//          }
          state = state.commit();
//          if ( AbstractReportProcessor.SHOW_ROLLBACKS ) {
//            logger.debug(
//              "Print: Post Commit: " + state.getProcessKey() + " " + state.getAdvanceHandler().getClass().getName() );
//          }

          // printLayoutStateToFile(state, true, isInRollBackMode);

          if ( pagebreaksSupported && fallBackState != restoreState ) {
            final DefaultOutputFunction commitableOutputFunction =
              (DefaultOutputFunction) state.getLayoutProcess().getOutputFunction();
            final Renderer commitableRenderer = commitableOutputFunction.getRenderer();
            commitableRenderer.applyRollbackInformation();
          }
        }
//        if (isInRollBackMode && rollbackProcessed) {
//          isInRollBackMode = false;
//        }
      }

      // We should never reach this point, if this function has been called by the PageStateList.
      //throw new ReportProcessingException("Content processing failed with an illegal state");
      return null;
    } catch ( ContentProcessingException e ) {
      throw new ReportProcessingException( "Content-Processing failed.", e );
    } finally {
      Prd5547.inst.endProcessPage();
    }
  }

  public boolean paginate() throws ReportProcessingException {
    fireProcessingStarted( new ReportProgressEvent( this ) );

    try {
      if ( isPaginated() == false ) {
        // Processes the whole report ..
        prepareReportProcessing();
      }
    } finally {
      fireProcessingFinished( new ReportProgressEvent( this ) );
    }
    return true;
  }

  public void processReport() throws ReportProcessingException {
    PerformanceLoggingStopWatch swGlobal =
      getPerformanceMonitorContext().createStopWatch( PerformanceTags.REPORT_PROCESSING );
    try {
      swGlobal.start();
      if ( AbstractReportProcessor.logger.isDebugEnabled() ) {
        AbstractReportProcessor.logger.debug( new MemoryUsageMessage( System.identityHashCode(
          Thread.currentThread() ) + ": Report processing time: Starting: " ) );
      }
      try {
        final long startTime = System.currentTimeMillis();

        fireProcessingStarted( new ReportProgressEvent( this ) );

        if ( isPaginated() == false ) {
          // Processes the whole report ..
          prepareReportProcessing();
        }
        
        DebugReporter.DR.ON = false; // stop trace 
        PerformanceLoggingStopWatch sw =
          getPerformanceMonitorContext().createStopWatch( PerformanceTags.REPORT_GENERATE );
        try {
          sw.start();
          final long paginateTime = System.currentTimeMillis();
          if ( AbstractReportProcessor.logger.isDebugEnabled() ) {
            AbstractReportProcessor.logger.debug( new MemoryUsageMessage
              ( System.identityHashCode( Thread.currentThread() ) +
                ": Report processing time: Pagination time: " + ( ( paginateTime - startTime ) / 1000.0 ) ) );
          }
          if ( getLogicalPageCount() == 0 ) {
            throw new EmptyReportException( "Report did not generate any content." );
          }

          // Start from scratch ...
          PageState state = getLogicalPageState( 0 );
          while ( state != null //&& Prd5547.inst.getProcessPageI() < 2 
              ) {
            state = processPage( state, true );
          }
          final long endTime = System.currentTimeMillis();
          if ( AbstractReportProcessor.logger.isDebugEnabled() ) {
            AbstractReportProcessor.logger.debug( new MemoryUsageMessage
              ( System.identityHashCode( Thread.currentThread() ) +
                ": Report processing time: " + ( ( endTime - startTime ) / 1000.0 ) ) );
          }
        } finally {
          sw.close();
        }
      } catch ( EmptyReportException re ) {
        throw re;
      } catch ( ReportProcessingException re ) {
        AbstractReportProcessor.logger.error( System.identityHashCode(
          Thread.currentThread() ) + ": Report processing failed.", re );
        throw re;
      } catch ( Exception e ) {
        AbstractReportProcessor.logger.error( System.identityHashCode(
          Thread.currentThread() ) + ": Report processing failed.", e );
        throw new ReportProcessingException( "Failed to process the report", e );
      }
      fireProcessingFinished( new ReportProgressEvent( this, getPhysicalPageCount() ) );
      if ( AbstractReportProcessor.logger.isDebugEnabled() ) {
        AbstractReportProcessor.logger.debug( System.identityHashCode(
          Thread.currentThread() ) + ": Report processing finished." );
      }
    } finally {
      swGlobal.close();
    }

  }

  public int getLogicalPageCount() {
    if ( logicalMapping == null ) {
      return -1;
    }
    return logicalMapping.size();
  }

  public int getPhysicalPageCount() {
    if ( physicalMapping == null ) {
      return -1;
    }
    return physicalMapping.size();
  }

  /**
   * Checks whether the output mode may generate pagebreaks. If we have to deal with pagebreaks, we may have to perform
   * roll-backs and commits to keep the pagebreaks in sync with the state-processing. This is ugly, expensive and you
   * better dont try this at home.
   * <p/>
   * The roll-back is done for paginated and flow-report outputs, but if we have no autmoatic and manual pagebreaks,
   * there is no need to even consider to roll-back to a state before the pagebreak (which will never occur).
   *
   * @return a flag indicating whether the output target supports pagebreaks.
   */
  private boolean isPagebreaksSupported() {
    return pagebreaksSupported;
  }
}
