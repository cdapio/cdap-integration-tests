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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a collection of strings and whether that is spam or not.
 */
public class LogisticRegressionSpamMessage {
  public static final String SPAM_PREDICTION_FIELD = "isSpam";
  public static final String IMP_FIELD = "imp";
  public static final String READ_FIELD = "boolField";
  static final Schema SCHEMA = Schema.recordOf("simpleMessage",
                                               Schema.Field.of(SPAM_PREDICTION_FIELD, Schema.of(Schema.Type.DOUBLE)),
                                               Schema.Field.of(IMP_FIELD, Schema.of(Schema.Type.INT)),
                                               Schema.Field.of(READ_FIELD, Schema.of(Schema.Type.INT))
  );

  private final Integer imp;
  private final Integer read;

  @Nullable
  private final Double spamPrediction;

  public LogisticRegressionSpamMessage(Integer imp, Integer read, @Nullable Double spamPrediction) {
    this.imp = imp;
    this.read = read;
    this.spamPrediction = spamPrediction;
  }

  @Override
  public int hashCode() {
    int result = imp.hashCode();
    result = 31 * result + read.hashCode();
    result = 31 * result + (spamPrediction != null ? spamPrediction.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogisticRegressionSpamMessage that = (LogisticRegressionSpamMessage) o;

    return Objects.equals(imp, that.imp) &&
      Objects.equals(read, that.read) &&
      Objects.equals(spamPrediction, that.spamPrediction);
  }
}
