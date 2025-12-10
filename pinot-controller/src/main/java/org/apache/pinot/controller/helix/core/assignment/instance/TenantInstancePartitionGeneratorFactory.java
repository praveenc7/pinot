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
package org.apache.pinot.controller.helix.core.assignment.instance;

import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating TenantInstancePartitionGenerator instances.
 *
 * This factory allows pluggable Tenant IP assignment strategies through configuration.
 *
 * Configuration:
 * - controller.tenant.instance.partition.generator.class: Full class name of generator implementation
 * - If not specified or class loading fails, falls back to DefaultTenantInstancePartitionGenerator
 *
 * Example custom configuration:
 * GENERATOR_CLASS_CONFIG_KEY=com.linkedin.pinot.controller.LiMZAwareTenantInstancePartitionGenerator
 */
public class TenantInstancePartitionGeneratorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantInstancePartitionGeneratorFactory.class);

  private TenantInstancePartitionGeneratorFactory() {
    // Private constructor to prevent instantiation
  }

  /**
   * Creates an instance of TenantInstancePartitionGenerator based on controller configuration
   * with PinotHelixResourceManager dependency injection.
   *
   * @param controllerConf Controller configuration containing generator class specification
   * @param pinotHelixResourceManager Resource manager for Helix operations (can be null)
   * @return TenantInstancePartitionGenerator instance (never null)
   */
  public static TenantInstancePartitionGenerator getInstance(ControllerConf controllerConf,
      PinotHelixResourceManager pinotHelixResourceManager) {
    String generatorClassName = controllerConf.getTenantInstancePartitionGeneratorClass();

    LOGGER.info("Creating TenantInstancePartitionGenerator using class: {}", generatorClassName);

    try {
      Class<?> generatorClass = Class.forName(generatorClassName);

      if (!TenantInstancePartitionGenerator.class.isAssignableFrom(generatorClass)) {
        throw new IllegalArgumentException("Class " + generatorClassName
            + " does not implement TenantInstancePartitionGenerator interface");
      }

      TenantInstancePartitionGenerator generator;

      try {
        generator = (TenantInstancePartitionGenerator) generatorClass
            .getDeclaredConstructor(PinotHelixResourceManager.class, ControllerConf.class)
            .newInstance(pinotHelixResourceManager, controllerConf);
        LOGGER.info("Successfully created TenantInstancePartitionGenerator with ResourceManager: {}",
            generatorClassName);
        return generator;
      } catch (NoSuchMethodException e) {
        LOGGER.debug("No constructor with PinotHelixResourceManager found for {}, trying default constructor",
            generatorClassName);
      }


      // Fallback to default constructor
      generator = new DefaultTenantInstancePartitionGenerator(pinotHelixResourceManager, controllerConf);
      LOGGER.info("Successfully created DefaultTenantInstancePartitionGenerator as fallback");
      return generator;
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate TenantInstancePartitionGenerator class: {}. "
          + "Falling back to default implementation", generatorClassName, e);

      // Fallback to default implementation with resource manager if available
      return new DefaultTenantInstancePartitionGenerator(pinotHelixResourceManager, controllerConf);
    }
  }
}
