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

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import co.cask.tracker.entity.TrackerMeterRequest;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Created by Yue on 7/14/16.
 */
public class logHandler  extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(logHandler.class);

  @Path("v1/loghost")
  @GET
  public void trackerMeter(HttpServiceRequest request, HttpServiceResponder responder) {
    LOG.info("HEADER TEST: " + request.getAllHeaders());
    LOG.info("host TEST: " + request.getHeader("host"));
    LOG.info("Host TEST: " + request.getHeader("Host"));
    responder.sendString(new Gson().toJson(request.getAllHeaders()));
  }
}

