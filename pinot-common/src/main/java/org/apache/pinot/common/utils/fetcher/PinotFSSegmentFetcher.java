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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;

public class PinotFSSegmentFetcher extends BaseSegmentFetcher {

  @Override
  protected void fetchSegmentToLocalWithoutRetry(URI uri, File dest) throws Exception {
    String scheme = (uri.getScheme() == null) ? PinotFSFactory.LOCAL_PINOT_FS_SCHEME : uri.getScheme();
    PinotFS fs = PinotFSFactory.create(scheme);
    fs.copyToLocalFile(uri, dest);
  }

  /**
   * Unified FS-based implementation that supports schemes like s3:// and https://.
   * Streams the tarball directly from PinotFS and untars into the destination directory.
   * Includes retries and full logging.
   */
  @Override
  public File fetchUntarSegmentToLocalStreamed(URI downloadURI, File destDir, long maxStreamRateInByte,
      AtomicInteger attempts) throws Exception {
    AtomicReference<File> result = new AtomicReference<>();

    _logger.info("Starting fetchUntarSegmentToLocalStreamed: URI={}, destDir={}, retries={}",
        downloadURI, destDir, _retryCount);

    int tries;
    try {
      tries = RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, _retryDelayScaleFactor)
          .attempt(() -> {
            try {
              String scheme = (downloadURI.getScheme() == null)
                  ? PinotFSFactory.LOCAL_PINOT_FS_SCHEME : downloadURI.getScheme();
              PinotFS fs = PinotFSFactory.create(scheme);

              _logger.info("Opening input stream from {} using PinotFS scheme: {}", downloadURI, scheme);
              try (InputStream inputStream = fs.open(downloadURI)) {
                File segmentRoot =
                    TarCompressionUtils.untarWithRateLimiter(inputStream, destDir, maxStreamRateInByte).get(0);

                result.set(segmentRoot);
              }

              return true;
            } catch (IOException e) {
              _logger.warn("IOException during untar fetch from {} to {}, will retry", downloadURI, destDir, e);
              return false;
            } catch (Exception e) {
              _logger.warn("Unexpected exception during fetch from {} to {}, will retry", downloadURI, destDir, e);
              return false;
            }
          });
    } catch (AttemptsExceededException | RetriableOperationException e) {
      attempts.set(e.getAttempts());
      throw e;
    }

    attempts.set(tries);
    return result.get();
  }
}
