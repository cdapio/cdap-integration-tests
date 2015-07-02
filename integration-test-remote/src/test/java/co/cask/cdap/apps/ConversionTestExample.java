package co.cask.cdap.apps;

import co.cask.cdap.api.app.AbstractApplication;

public class ConversionTestExample extends AbstractApplication {
  @Override
  public void configure() {
    setName("ConversionTestExample");
    setDescription("Application used to test ETL conversions");
    addService(new ConversionTestService());
  }
}
