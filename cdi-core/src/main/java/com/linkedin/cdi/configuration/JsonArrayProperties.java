// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.gson.JsonArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.StaticConstants.*;


/**
 * A JsonArray type of property has default defaultValue of "[]"
 */
public class JsonArrayProperties extends MultistageProperties<JsonArray> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonArrayProperties.class);

  JsonArrayProperties(String config) {
    super(config, JsonArray.class, new JsonArray());
  }

  JsonArrayProperties(String config, JsonArray defaultValue) {
    super(config, JsonArray.class, defaultValue);
  }

  /**
   * Validates the value when it is blank
   * - No configuration is considered blank
   * - A blank string is considered blank
   * - An empty array [] is considered blank
   *
   * @param state state
   * @return true if blank
   */
  @Override
  public boolean isBlank(State state) {
    if (!state.contains(getConfig())) {
      return true;
    }

    if (StringUtils.isBlank(state.getProp(getConfig()))) {
      return true;
    }

    try {
      return GSON.fromJson(state.getProp(getConfig()), JsonArray.class).size() == 0;
    } catch (Exception e) {
      LOG.error(getConfig(), e.getMessage());
      return false;
    }
  }

  /**
   * Validates the value when it is non-blank and accepts blank value
   * - A blank configuration is considered valid
   * - Any properly formed JSON array is considered valid
   * @param state state
   * @return true if blank or non-blank and valid
   */
  @Override
  public boolean isValid(State state) {
    if (!isBlank(state)) try {
      // Properly formed JsonArray string is valid
      GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
    } catch (Exception e) {
      LOG.error(getConfig(), e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Validates the value when it is non-blank and rejects blank value, including blank array "[]"
   * - only properly formed JSON array with at least 1 item is considered valid
   *
   * @param state source state
   * @return true when the configuration is non-blank and valid
   */
  public boolean validateNonblank(State state) {
    if (!isBlank(state) && isValid(state)) {
      return GSON.fromJson(state.getProp(getConfig()), JsonArray.class).size() > 0;
    }
    return false;
  }

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value
   *
   * This method raises an exception when a non-conforming value is configured.
   *
   * @param state state
   * @return property value if non-blank and valid, otherwise the default value
   */
  @Override
  public JsonArray getProp(State state) {
    if (validateNonblank(state)) {
      return GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
    }
    return getDefaultValueWithCheck(state);
  }

  /**
   * Retrieves property value from state object if valid and not blank
   * otherwise, return default value
   *
   * This method will keep quiet if the configuration is incorrect
   *
   * @param state state
   * @return property value if non-blank and valid, otherwise the default value
   */
  public JsonArray getValidNonblankWithDefault(State state) {
    if (validateNonblank(state)) {
      return GSON.fromJson(state.getProp(getConfig()), JsonArray.class);
    }
    return getDefaultValue();
  }
}
