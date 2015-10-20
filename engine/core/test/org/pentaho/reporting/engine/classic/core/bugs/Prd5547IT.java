/*
 * This program is free software; you can redistribute it and/or modify it under the
 *  terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 *  Foundation.
 *
 *  You should have received a copy of the GNU Lesser General Public License along with this
 *  program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 *  or from the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 *
 *  Copyright (c) 2006 - 2015 Pentaho Corporation..  All rights reserved.
 */

package org.pentaho.reporting.engine.classic.core.bugs;

import static javax.xml.xpath.XPathConstants.NODESET;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.reporting.engine.classic.core.ClassicEngineBoot;
import org.pentaho.reporting.engine.classic.core.MasterReport;
import org.pentaho.reporting.engine.classic.core.ReportProcessingException;
import org.pentaho.reporting.engine.classic.core.modules.output.pageable.base.PageableReportProcessor;
import org.pentaho.reporting.engine.classic.core.modules.output.pageable.xml.XmlPageOutputProcessor;
import org.pentaho.reporting.libraries.resourceloader.Resource;
import org.pentaho.reporting.libraries.resourceloader.ResourceCreationException;
import org.pentaho.reporting.libraries.resourceloader.ResourceException;
import org.pentaho.reporting.libraries.resourceloader.ResourceKeyCreationException;
import org.pentaho.reporting.libraries.resourceloader.ResourceLoadingException;
import org.pentaho.reporting.libraries.resourceloader.ResourceManager;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class Prd5547IT {

  @Before
  public void before() throws IOException {
    ClassicEngineBoot.getInstance().start();
  }

  @Test
  public void testTableLayout_singleHeader() throws Exception {
    final URL url = getClass().getResource( "Prd5547IT_singleHdr.prpt" );

    String xml = runReportToXmlPage( url );

    final String[] actualTexts = extractTexts( xml );

    String[] expectedTexts = new String[] {//
        "HEADER_200_single",//
          "ROWNUM-1_H190_single_header",//
          "ROWNUM-2_H190_single_header",//
          "1",//
          "ROWNUM-3_H190_single_header",//
          "ROWNUM-4_H190_single_header",//
          "ROWNUM-5_H190_single_header",//
          "2",//
          "ROWNUM-6_H190_single_header",//
          "3"//
        };
//    actualTexts before fix
//    HEADER_200_single
//    ROWNUM-1_H190_single_header
//    ROWNUM-2_H190_single_header
//    1
//    HEADER_200_single
//    ROWNUM-3_H190_single_header
//    ROWNUM-4_H190_single_header
//    2
//    HEADER_200_single
//    ROWNUM-6_H190_single_header
//    3
//    HEADER_200_single
//    4
    Assert.assertArrayEquals( expectedTexts, actualTexts );
  }

  @Test
  public void testTableLayout_repeatHeader() throws Exception {
    final URL url = getClass().getResource( "Prd5547IT_repeatHdr.prpt" );

    String xml = runReportToXmlPage( url );

    final String[] actualTexts = extractTexts( xml );

    String[] expectedTexts = new String[] {//
        "HEADER_200_repeat",//
          "ROWNUM-1_H190_repeat_header",//
          "ROWNUM-2_H190_repeat_header",//
          "1",//
          "HEADER_200_repeat",//
          "ROWNUM-3_H190_repeat_header",//
          "ROWNUM-4_H190_repeat_header",//
          "2",//
          "HEADER_200_repeat",//
          "ROWNUM-5_H190_repeat_header",//
          "ROWNUM-6_H190_repeat_header",//
          "3"//
        };
//    actualTexts before fix
//    HEADER_200_repeat
//    ROWNUM-1_H190_repeate_header
//    ROWNUM-2_H190_repeate_header
//    1
//    HEADER_200_repeat
//    ROWNUM-3_H190_repeate_header
//    ROWNUM-4_H190_repeate_header
//    2
//    HEADER_200_repeat
//    ROWNUM-6_H190_repeate_header
//    3
//    HEADER_200_repeat
//    4

    Assert.assertArrayEquals( expectedTexts, actualTexts );
  }

  private String runReportToXmlPage( final URL url ) throws ResourceLoadingException, ResourceCreationException,
    ResourceKeyCreationException, ResourceException, ReportProcessingException, IOException {
    System.out.println( "url=" + url );
    assertNotNull( url );
    ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();

    final ResourceManager resourceManager = new ResourceManager();
    resourceManager.registerDefaults();
    final Resource directly = resourceManager.createDirectly( url, MasterReport.class );
    final MasterReport report = (MasterReport) directly.getResource();
    XmlPageOutputProcessor outputProcessor = new XmlPageOutputProcessor( report.getConfiguration(), xmlOutputStream );
    final PageableReportProcessor proc = new PageableReportProcessor( report, outputProcessor );
    proc.processReport();
    xmlOutputStream.close();

    String xml = org.apache.commons.io.IOUtils.toString( xmlOutputStream.toByteArray(), "UTF-8" );

    // {// TODO: for debug only
    // final File outFile = File.createTempFile( "reportOut-", "", new File( "test-output\\" ) );
    // copyToFile( xmlOutputStream.toByteArray(), outFile );
    // }
    return xml;
  }

  private String[] extractTexts( String xml ) throws XPathExpressionException {
    final String xpathExpr = "//*[name()=\"text\"]/text()";
    XPath xpath = XPathFactory.newInstance().newXPath();
    final InputSource xmlSource = new InputSource( new StringReader( xml ) );
    NodeList r = (NodeList) xpath.evaluate( xpathExpr, xmlSource, NODESET );
    final int cnt = r.getLength();
    final String[] texts = new String[cnt];
    for ( int i = 0, n = cnt; i < n; i++ ) {
      texts[i] = r.item( i ).getTextContent();
      System.out.println( i + ": " + texts[i] );
    }
    return texts;
  }

//  // TODO: for debug only
//  private void copyToFile( byte[] bytes, final File outFile ) throws IOException {
//    FileOutputStream out = null;
//    try {
//      System.out.println( outFile.getAbsolutePath() );
//      out = new FileOutputStream( outFile );
//      out.write( bytes );
//    } catch ( Exception e ) {
//      if ( out != null ) {
//        out.close();
//      }
//    }
//  }

}
