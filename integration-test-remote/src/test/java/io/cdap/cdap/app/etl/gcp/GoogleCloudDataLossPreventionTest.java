package io.cdap.cdap.app.etl.gcp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InfoTypeTransformations.InfoTypeTransformation;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.RedactConfig;
import com.google.privacy.dlp.v2.ReplaceValueConfig;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GoogleCloudDataLossPreventionTest extends DataprocETLTestBase {
  private static DlpServiceClient client;

  private static class DlpCredentialsProvider implements CredentialsProvider {
    private Credentials credentials;

    public DlpCredentialsProvider(Credentials credentials) {
      this.credentials = credentials;
    }

    public Credentials getCredentials() {
      return credentials;
    }
  }

  @Override
  protected void innerSetup() {

  }

  @Override
  protected void innerTearDown() {

  }

  @BeforeClass
  public static void setupGoogleDlp() throws Exception {
    GoogleCredentials credentials;
    try (InputStream is = new ByteArrayInputStream(getServiceAccountCredentials().getBytes(
        StandardCharsets.UTF_8))) {
      credentials = GoogleCredentials.fromStream(is);
    }
    CredentialsProvider provider = new DlpCredentialsProvider(credentials);
    DlpServiceSettings settings = DlpServiceSettings.newBuilder()
        .setCredentialsProvider(provider).build();
    client = DlpServiceClient.create(settings);
  }

  @Test
  public void replaceValueTransform() throws Exception {

    String EXPECTED = "I can't believe this news! My SSN is [SSN REDACTED]";

    ReplaceValueConfig replaceValueConfig = ReplaceValueConfig.newBuilder()
        .setNewValue(Value.newBuilder().setStringValue("[SSN REDACTED]").build()).build();

    PrimitiveTransformation primTransform = PrimitiveTransformation.newBuilder()
        .setReplaceConfig(replaceValueConfig).build();

    DeidentifyContentRequest request = getDeidentifyContentRequestFromPrimitiveTransform(
      primTransform);
    DeidentifyContentResponse deidentifyContentResponse = client.deidentifyContent(request);

    String res = deidentifyContentResponse.getItem().getTable().getRows(0)
        .getValues(0).getStringValue();
    Assert.assertEquals(EXPECTED, res);
  }

  @Test
  public void redactTransform() throws Exception {
    String EXPECTED = "I can't believe this news! My SSN is ";

    RedactConfig redactConfig = RedactConfig.newBuilder().build();

    PrimitiveTransformation primTransform = PrimitiveTransformation.newBuilder()
      .setRedactConfig(redactConfig).build();

    DeidentifyContentRequest request = getDeidentifyContentRequestFromPrimitiveTransform(
      primTransform);

    DeidentifyContentResponse deidentifyContentResponse = client.deidentifyContent(request);

    String res = deidentifyContentResponse.getItem().getTable().getRows(0)
      .getValues(0).getStringValue();
    Assert.assertEquals(EXPECTED, res);
  }

  private DeidentifyContentRequest getDeidentifyContentRequestFromPrimitiveTransform(PrimitiveTransformation trans){
    String REDACTION_TYPE = "US_SOCIAL_SECURITY_NUMBER";
    String BODY = "I can't believe this news! My SSN is 123-45-6789";
    FieldId field = FieldId.newBuilder().setName("body").build();

    InfoType infoType = InfoType.newBuilder().setName(REDACTION_TYPE).build();
    InfoTypeTransformation infoTypeTransformation =
      InfoTypeTransformation.newBuilder().setPrimitiveTransformation(trans).addInfoTypes(infoType).build();
    FieldTransformation fieldTransform = FieldTransformation.newBuilder()
      .addFields(field).setInfoTypeTransformations(InfoTypeTransformations.newBuilder().addTransformations(infoTypeTransformation).build()).build();
    RecordTransformations transforms = RecordTransformations.newBuilder()
      .addFieldTransformations(fieldTransform).build();
    DeidentifyConfig config = DeidentifyConfig.newBuilder()
      .setRecordTransformations(transforms).build();
    Table table = Table.newBuilder()
      .addHeaders(field)
      .addRows(Row.newBuilder()
                 .addValues(Value.newBuilder().setStringValue(BODY)))
      .build();
    ContentItem item = ContentItem.newBuilder().setTable(table).build();
    InspectConfig inspectConfig = InspectConfig.newBuilder().addInfoTypes(
      infoType).build();
    DeidentifyContentRequest request = DeidentifyContentRequest.newBuilder()
      .setInspectConfig(inspectConfig)
      .setParent("projects/" + getProjectId())
      .setDeidentifyConfig(config)
      .setItem(item).build();
    return request;
  }

}
