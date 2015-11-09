package org.pentaho.reporting.engine.classic.core.debug;

import java.io.PrintStream;
import java.io.Reader;

import bsh.ConsoleInterface;
import bsh.EvalError;
import bsh.Interpreter;
import bsh.NameSpace;

public class BSh {
  Interpreter interpreter = new Interpreter();
  Object[] lastResult = new Object[0];

  public BSh( Interpreter interpreter ) {
    super();
    this.interpreter = interpreter;
  }

  public BSh() {
    this( new Interpreter() );
  }

  public static BSh create() {
    return new BSh();
  }

  public static BSh init( Interpreter interpreter ) {
    return new BSh( interpreter );
  }

  public Interpreter getInterpreter() {
    return interpreter;
  }

  public BSh setConsole( ConsoleInterface paramConsoleInterface ) {
    interpreter.setConsole( paramConsoleInterface );
    return this;
  }

  public BSh setNameSpace( NameSpace paramNameSpace ) {
    interpreter.setNameSpace( paramNameSpace );
    return this;
  }

  public BSh run() {
    interpreter.run();
    return this;
  }

  public BSh eval( Reader paramReader, NameSpace paramNameSpace, String paramString ) throws EvalError {
    lastResult = new Object[] { interpreter.eval( paramReader, paramNameSpace, paramString ) };
    return this;
  }

  public BSh eval( Reader paramReader ) throws EvalError {
    lastResult = new Object[] { interpreter.eval( paramReader ) };
    return this;
  }

  public BSh eval( String paramString ) throws EvalError {
    lastResult = new Object[] { interpreter.eval( paramString ) };
    return this;
  }

  public BSh eval( String paramString, NameSpace paramNameSpace ) throws EvalError {
    lastResult = new Object[] { interpreter.eval( paramString, paramNameSpace ) };
    return this;
  }

  public BSh set( String paramString, Object paramObject ) throws EvalError {
    interpreter.set( paramString, paramObject );
    return this;
  }

  public BSh set( String paramString, long paramLong ) throws EvalError {
    interpreter.set( paramString, paramLong );
    return this;
  }

  public BSh set( String paramString, int paramInt ) throws EvalError {
    interpreter.set( paramString, paramInt );
    return this;
  }

  public BSh set( String paramString, double paramDouble ) throws EvalError {
    interpreter.set( paramString, paramDouble );
    return this;
  }

  public BSh set( String paramString, float paramFloat ) throws EvalError {
    interpreter.set( paramString, paramFloat );
    return this;
  }

  public BSh set( String paramString, boolean paramBoolean ) throws EvalError {
    interpreter.set( paramString, paramBoolean );
    return this;
  }

  public BSh unset( String paramString ) throws EvalError {
    interpreter.unset( paramString );
    return this;
  }

  public BSh setClassLoader( ClassLoader paramClassLoader ) {
    interpreter.setClassLoader( paramClassLoader );
    return this;
  }

  public BSh setStrictJava( boolean paramBoolean ) {
    interpreter.setStrictJava( paramBoolean );
    return this;
  }

  public BSh setOut( PrintStream paramPrintStream ) {
    interpreter.setOut( paramPrintStream );
    return this;
  }

  public BSh setErr( PrintStream paramPrintStream ) {
    interpreter.setErr( paramPrintStream );
    return this;
  }

  public BSh setExitOnEOF( boolean paramBoolean ) {
    interpreter.setExitOnEOF( paramBoolean );
    return this;
  }

}
