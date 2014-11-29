package co.cask.cdap.examples;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Example Procedure used to test Service discovery and dataset keys written.
 */
public class ServiceProcedure extends AbstractProcedure {

  @UseDataSet(ServiceApplication.WORKER_DATASET_NAME)
  KeyValueTable table;

  @Handle("pingService")
  @SuppressWarnings("UnusedDeclaration")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    URL serviceURL = getContext().getServiceURL(ServiceApplication.SERVICE_NAME);

    URL pingURL;
    try {
      pingURL = new URL(serviceURL, "ping");
    } catch (Exception e) {
      responder.error(ProcedureResponse.Code.FAILURE, "Malformed URL Exception.");
      return;
    }

    HttpURLConnection urlConnection = (HttpURLConnection) pingURL.openConnection();
    urlConnection.connect();
    int statusCode = urlConnection.getResponseCode();

    if (statusCode == 200) {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, "OK");
    } else {
      responder.error(ProcedureResponse.Code.FAILURE, "Could not discover service.");
    }
  }

  @Handle("datasetWorker")
  @SuppressWarnings("UnusedDeclaration")
  public void datasetWorker(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String value = Bytes.toString(table.read(HttpService.WORKER_DATASET_TEST_KEY));
    if (value == null || !value.equals(HttpService.WORKER_DATASET_TEST_VALUE)) {
      responder.error(ProcedureResponse.Code.FAILURE, "Incorrect dataset value.");
    } else {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, "OK");
    }
  }

}
