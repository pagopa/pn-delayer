package it.pagopa.pn.delayer.middleware.dao;

import java.io.IOException;
import java.util.List;

public interface DeliveryDriverProvincePartitionDAO {

    List<String> retrievePartition() throws IOException;
}
