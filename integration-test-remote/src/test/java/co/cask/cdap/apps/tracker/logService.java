/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.apps.tracker;

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.common.http.HttpRequest;
import co.cask.tracker.AuditLogHandler;
import co.cask.tracker.AuditMetricsHandler;
import co.cask.tracker.AuditTagsHandler;
import co.cask.tracker.TrackerMeterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by Yue on 7/14/16.
 */
public class logService extends AbstractService {
  public static final String SERVICE_NAME = "logService";

  protected void configure() {
    this.setName("logService");
    this.setDescription("A service to log data.");
    this.addHandler(new logHandler());
  }
}
