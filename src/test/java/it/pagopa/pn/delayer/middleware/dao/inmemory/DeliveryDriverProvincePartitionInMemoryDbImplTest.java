package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

@ExtendWith(MockitoExtension.class)
class DeliveryDriverProvincePartitionInMemoryDbImplTest {

    private DeliveryDriverProvincePartitionInMemoryDbImpl dao;

    @BeforeEach
    void setUp() {
        dao = new DeliveryDriverProvincePartitionInMemoryDbImpl(new ObjectMapper());
    }

    @Test
    void retrievePartition_returnsCorrectPartitionList() {
        List<String> result = dao.retrievePartition();
        Assertions.assertEquals(636, result.size());
    }
}