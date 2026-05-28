/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URL;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.AdditionalAnswers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class PinotQueryResourceTest {

  @Mock
  PinotHelixResourceManager _resourceManager;
  @Mock
  TableCache _tableCache;
  @Mock
  Schema _schema;
  @Mock
  AccessControlFactory _accessControlFactory;
  @Mock
  ControllerConf _controllerConf;
  @InjectMocks
  PinotQueryResource _pinotQueryResource;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(_tableCache.getActualTableName(any())).then(AdditionalAnswers.returnsFirstArg());
    when(_resourceManager.getTableCache()).thenReturn(_tableCache);
    when(_tableCache.getSchema(any())).thenReturn(_schema);
  }

  @Test
  public void testV2QueryOnV1() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("WITH tmp AS (SELECT * FROM a) SELECT * FROM tmp", null, null, null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())));
    Assert.assertTrue(response.contains("retry the query using the multi-stage query engine"));
  }

  @Test
  public void testInvalidQuery() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("INVALID QUERY", null, null, null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())));
    Assert.assertFalse(response.contains("retry the query using the multi-stage query engine"));
  }

  @Test
  public void testEncodeHostForUrlHostname() {
    Assert.assertEquals(PinotQueryResource.encodeHostForUrl("broker-0.pinot.svc"), "broker-0.pinot.svc");
  }

  @Test
  public void testEncodeHostForUrlIpv4() {
    Assert.assertEquals(PinotQueryResource.encodeHostForUrl("10.20.30.40"), "10.20.30.40");
  }

  @Test
  public void testEncodeHostForUrlIpv6() {
    // Guava normalizes to RFC 5952 canonical form (zero-runs compressed with "::").
    Assert.assertEquals(PinotQueryResource.encodeHostForUrl("2a04:f547:4a:706:0:0:0:e1ae"),
        "[2a04:f547:4a:706::e1ae]");
  }

  @Test
  public void testEncodeHostForUrlGarbageHostname() {
    // Strings that look almost-IPv6 but are invalid stay untouched (no brackets added).
    Assert.assertEquals(PinotQueryResource.encodeHostForUrl("not:a:real:address"), "not:a:real:address");
  }

  @Test
  public void testEncodeHostForUrlIpv6Loopback() {
    Assert.assertEquals(PinotQueryResource.encodeHostForUrl("::1"), "[::1]");
  }

  @Test
  public void testEncodeHostForUrlAlreadyBracketedIpv6() {
    Assert.assertEquals(PinotQueryResource.encodeHostForUrl("[2a04:f547:4a:706:0:0:0:e1ae]"),
        "[2a04:f547:4a:706:0:0:0:e1ae]");
  }

  // Reproduces the failure: building a broker URL from an IPv6 host without brackets makes
  // new URL(...) throw because the IPv6 colons collide with the port separator.
  @Test
  public void testIpv6BrokerUrlIsParseable()
      throws Exception {
    String urlStr = String.format("https://%s:%d/query/sql",
        PinotQueryResource.encodeHostForUrl("2a04:f547:4a:706:0:0:0:e1ae"), 8099);
    URL url = new URL(urlStr);
    URI uri = url.toURI();
    Assert.assertEquals(uri.getHost(), "[2a04:f547:4a:706::e1ae]");
    Assert.assertEquals(uri.getPort(), 8099);
    Assert.assertEquals(uri.getPath(), "/query/sql");
  }

  public static String streamingOutputToString(StreamingOutput streamingOutput) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      streamingOutput.write(byteArrayOutputStream);
      return byteArrayOutputStream.toString();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while converting StreamingOutput to String", e);
    }
  }
}
