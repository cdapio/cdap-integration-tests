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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.data.schema.Schema;

/**
 * Represents a string and whether that is spam or not.
 */
public class SpamMessage {
  public static final String SPAM_PREDICTION_FIELD = "isSpam";
  public static final String TEXT_FIELD = "text";
  static final Schema SCHEMA = Schema.recordOf(
    "simpleMessage",
    Schema.Field.of(SPAM_PREDICTION_FIELD, Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of(TEXT_FIELD, Schema.of(Schema.Type.STRING))
  );

  private final String text;
  private final Double spamPrediction;

  public SpamMessage(String text, Double spamPrediction) {
    this.text = text;
    this.spamPrediction = spamPrediction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SpamMessage that = (SpamMessage) o;

    if (!text.equals(that.text)) {
      return false;
    }
    return !(spamPrediction != null ? !spamPrediction.equals(that.spamPrediction) : that.spamPrediction != null);

  }

  @Override
  public int hashCode() {
    int result = text.hashCode();
    result = 31 * result + (spamPrediction != null ? spamPrediction.hashCode() : 0);
    return result;
  }
}
