package org.pentaho.reporting.engine.classic.core.debug;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.pentaho.reporting.engine.classic.core.layout.model.LayoutNodeTypes;
import org.pentaho.reporting.engine.classic.core.layout.model.LogicalPageBox;
import org.pentaho.reporting.engine.classic.core.layout.model.ParagraphPoolBox;
import org.pentaho.reporting.engine.classic.core.layout.model.ParagraphRenderBox;
import org.pentaho.reporting.engine.classic.core.layout.model.RenderBox;
import org.pentaho.reporting.engine.classic.core.layout.model.RenderNode;
import org.pentaho.reporting.engine.classic.core.layout.model.RenderableText;
import org.pentaho.reporting.engine.classic.core.layout.model.table.TableRowRenderBox;
import org.pentaho.reporting.engine.classic.core.layout.model.table.TableSectionRenderBox;

public class DebugUtil {

  public static int MAX_BRANCH_NEST = 20;

  public static final String EOLN = "\r\n";
  public static boolean FILTER = false;
  // public static boolean FILTER = false;
  public static boolean displayParent = false;
  public static boolean displayParagraphPool = true;
  public static boolean displayPageHeader = true;
  public static boolean displayPageFooter = false;

  public static String display( RenderNode node, boolean full ) {
    StringBuilder sb = new StringBuilder();
    appendBranch( sb, node, 0, full );
    return sb.toString();
  }

  public static String display( RenderNode node ) {
    return display( node, false );
  }

  public static StringBuilder appendBranch( StringBuilder sb, RenderNode node, int shift, boolean full ) {
    return appendBranch( sb, node, shift, full, null );
  }

  public static String getNodeFilter( RenderNode node ) {
    // return null;
    if ( node instanceof RenderBox ) {
      long q = 10000L;
      StringBuilder sb = new StringBuilder();
      RenderBox box = (RenderBox) node;
      sb// .append( "box[ofAH=" ).append( box.getOverflowAreaHeight() / q )
      .append( ";seen=" ).append( box.isMarkedSeen() ? "M" : "-" ).append( box.isAppliedSeen() ? "A" : "-" ).append(
          "]" );
      sb.append( "[" ).append( "y=" ).append( node.getY() / q )
      // .append( "; " )
      // .append("cY=" )
          .append( "/" ).append( node.getCachedY() / q ).append( "; " ).append( "h=" ).append( node.getHeight() / q )
          // .append( "; " )
          // .append("cH=" )
          .append( "/" ).append( node.getCachedHeight() / q ).append( "; " ).append( "cs=" ).append(
              node.getCacheState() ).append( "; " )
          // .append("nt=" ).append( getNodeTypeName(node.getNodeType()) ).append( "; " )
          .append( "]" );
      return sb.toString();
    } else {
      return null;
    }
  }

  public static StringBuilder appendBranch( StringBuilder sb, RenderNode node, int shift, boolean full, String filter ) {
    if ( sb == null )
      sb = new StringBuilder();
    if ( shift > MAX_BRANCH_NEST ) {
      appendShift( sb, shift );
      sb.append( ">>>" );
      appendEoln( sb );
      return sb;
    }

    String nodeFilter = getNodeFilter( node );
    boolean printIt = true;
    if ( FILTER ) {
      if ( shift > 0 && node instanceof RenderBox ) {
        RenderBox box = (RenderBox) node;
        if ( node.getParent() != null && node.getNext() == null && node.getPrev() == null && box.getChildCount() > 0 ) {
          if ( filter != null && filter.equals( nodeFilter ) ) {
            printIt = false;
          }
        }
      } else {
      }
    }
    if ( !printIt ) {
      return sb;
    }
    {
      appendShift( sb, shift );
      appendNode( sb, node, full );
      appendEoln( sb );
    }
    if ( displayParent ) {
      RenderBox parent = node.getParent();
      if ( parent != null ) {
        {
          appendShift( sb, shift + 1 );
          sb.append( "PARENT. " );
          appendNode( sb, parent, full );
          appendEoln( sb );
        }
        // append( sb, parent, shift + 2, full, nodeFilter );
      }
    }
    if ( node instanceof ParagraphRenderBox ) {
      if ( displayParagraphPool ) {
        {
          appendShift( sb, shift + 1 );
          sb.append( "POOL" );
          appendEoln( sb );
        }
        ParagraphRenderBox para = (ParagraphRenderBox) node;
        ParagraphPoolBox poolBox = para.getPool();
        RenderNode poolBoxNode = poolBox;
        appendBranch( sb, poolBoxNode, shift + 2, full, nodeFilter );
      }
    }

    if ( node instanceof LogicalPageBox ) {
      LogicalPageBox page = (LogicalPageBox) node;
      if ( displayPageHeader ) {
        {
          appendShift( sb, shift + 1 );
          sb.append( "HEADER" );
          appendEoln( sb );
        }
        RenderBox header = page.getHeaderArea();
        appendBranch( sb, header, shift + 2, full, nodeFilter );
      }
      if ( displayPageFooter ) {
        {
          appendShift( sb, shift + 1 );
          sb.append( "FOOTER" );
          appendEoln( sb );
        }
        RenderBox footer = page.getFooterArea();
        appendBranch( sb, footer, shift + 2, full, nodeFilter );
      }
      if ( displayPageFooter ) {
        {
          appendShift( sb, shift + 1 );
          sb.append( "R-FOOTER" );
          appendEoln( sb );
        }
        RenderBox header = page.getRepeatFooterArea();
        appendBranch( sb, header, shift + 2, full, nodeFilter );
      }
    }
    if ( node instanceof RenderBox ) {
      RenderBox box = (RenderBox) node;
      node = box.getFirstChild();
      while ( node != null ) {
        appendBranch( sb, node, shift + 1, full, nodeFilter );
        node = node.getNext();
      }
    }
    return sb;
  }

  public static StringBuilder appendEoln( StringBuilder sb ) {
    if ( sb == null )
      sb = new StringBuilder();
    sb.append( EOLN );
    return sb;
  }

  public static StringBuilder appendNode( StringBuilder sb, RenderNode node, boolean full ) {
    long q = 10000L;
    if ( sb == null )
      sb = new StringBuilder();
    if ( node != null ) {
      if ( node instanceof RenderBox ) {
        RenderBox box = (RenderBox) node;
        sb.append( "box[ofAH=" ).append( box.getOverflowAreaHeight() / q ).append( ";seen=" ).append(
            box.isMarkedSeen() ? "M" : "-" )
        // .append("/")
            .append( box.isAppliedSeen() ? "A" : "-"  )
            .append(" p=").append( (box.isContainsReservedContent()?"T":"_") )
            .append( "]" );
      }
      sb.append( "[" ).append( "y=" )
          .append( node.getY() / q )
          // .append( "; " )
          // .append("cY=" )
          .append( "/" ).append( node.getCachedY() / q ).append( "; " ).append( "h=" )
          .append( node.getHeight() / q )
          // .append( "; " )
          // .append("cH=" )
          .append( "/" ).append( node.getCachedHeight() / q ).append( "; " ).append( "cs=" ).append(
              node.getCacheState() ).append( "; " ).append( "nt=" ).append( getNodeTypeName( node.getNodeType() ) )
          .append( "; " ).append( "lnt=" ).append( getNodeTypeName( node.getLayoutNodeType() ) ).append( "; " ).append(
              "]" );
      if ( node instanceof TableSectionRenderBox ) {
        sb.append( "[s-role=" ).append( ( (TableSectionRenderBox) node ).getDisplayRole() ).append( "]" );
      }

      sb.append( " " ).append( node.getClass().getSimpleName() );
      if ( node instanceof RenderableText ) {
        RenderableText txt = (RenderableText) node;
        sb.append( "\"" ).append( txt.getRawText() ).append( "\"" );
      }
      if ( node instanceof TableRowRenderBox ) {
        TableRowRenderBox row = (TableRowRenderBox) node;
        sb.append( "(row=" ).append( row.getRowIndex() ).append( ")" );
      }
      if ( node instanceof LogicalPageBox ) {
        LogicalPageBox page = (LogicalPageBox) node;
        sb.append( "(page=" ).append( page.getName() ).append( " off=" ).append( page.getPageOffset() ).append( ")" );
      }
      sb.append( " " );
      if ( full ) {
        sb.append( String.valueOf( node ) );
      }
    } else {
      sb.append( "null" );
    }
    return sb;
  }

  public static StringBuilder appendShift( StringBuilder sb, int shift ) {
    if ( sb == null )
      sb = new StringBuilder();
    for ( int i = 0; i < shift; i++ ) {
      sb.append( ". " );
    }
    return sb;
  }

  public static RenderNode getRoot( RenderNode node ) {
    if ( node == null )
      return null;
    while ( node.getParent() != null ) {
      node = node.getParent();
    }
    return node;
  }

  public static String getNodeTypeName( int value ) {
    StringBuilder ret = new StringBuilder();
    try {
      Class cls = LayoutNodeTypes.class;
      Field[] ff = cls.getFields();
      for ( Field f : ff ) {
        if ( f.getName().startsWith( "TYPE_" ) && Modifier.isStatic( f.getModifiers() )
            && f.getType().equals( int.class ) ) {
          int fVal = (Integer) f.get( null );
          if ( fVal == value ) {
            ret.append( f.getName() );
            break;
          }
        }
      }
      for ( Field f : ff ) {
        if ( f.getName().startsWith( "MASK_" ) && Modifier.isStatic( f.getModifiers() )
            && f.getType().equals( int.class ) ) {
          int fVal = (Integer) f.get( null );
          if ( fVal == ( value & fVal ) ) {
            ret.append( ":" ).append( f.getName().substring( 4 ) );
          }
        }
      }
      return ret.toString();
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }
}
