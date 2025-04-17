package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.delayer.middleware.dao.DeliveryDriverProvincePartitionDAO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeliveryDriverProvincePartitionInMemoryDbImpl implements DeliveryDriverProvincePartitionDAO {

    private final ObjectMapper objectMapper;

    @Override
    public List<String> retrievePartition() {
        ClassPathResource classPathResource = new ClassPathResource("json/DeliveryDriverProvinceParameter.json");
        Map<String, List<String>> partionList;
        try {
            partionList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {});
            log.info("retrieved {} partitions", partionList.size());
        } catch (IOException e) {
            throw new PnInternalException(e.getMessage(), "GENERIC_ERROR");
        }
        return createDeliveryDriverProvincePartition(partionList);
    }

    private List<String> createDeliveryDriverProvincePartition(Map<String, List<String>> partionList) {
        return partionList.entrySet().stream()
                .map(entry -> entry.getValue().stream().map(s -> entry.getKey() + "~" + s).toList())
                .flatMap(List::stream)
                .toList();
    }
}
