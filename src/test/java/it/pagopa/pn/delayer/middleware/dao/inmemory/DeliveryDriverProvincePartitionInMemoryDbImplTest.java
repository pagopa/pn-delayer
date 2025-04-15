package it.pagopa.pn.delayer.middleware.dao.inmemory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DeliveryDriverProvincePartitionInMemoryDbImplTest {

    private final DeliveryDriverProvincePartitionInMemoryDbImpl dao = new DeliveryDriverProvincePartitionInMemoryDbImpl();

    @Test
    void retrievePartition_returnsCorrectPartitionList() {
        List<String> result = dao.retrievePartition();
        Assertions.assertEquals(636, result.size());
    }
}