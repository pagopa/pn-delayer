package it.pagopa.pn.delayer.middleware.dao.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class PaperDeliveryHighPriorityInMemoryDbImpl implements PaperDeliveryHighPriorityDAO {

    private final ConcurrentHashMap<String, List<PaperDeliveryHighPriority>> data = new ConcurrentHashMap<>();

    public PaperDeliveryHighPriorityInMemoryDbImpl() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryHighPriority.json");
        List<PaperDeliveryHighPriority> highPriorityList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {});
        data.putAll(highPriorityList.stream()
                .collect(Collectors.groupingBy(
                        PaperDeliveryHighPriority::getPk,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(item -> Instant.parse(item.getCreatedAt())));
                                    return list;
                                }
                        )
                )));
    }

    @Override
    public Mono<Integer> delete(String pk, List<PaperDeliveryHighPriority> toRemove) {
        data.compute(pk, (k, existingList) -> {
            if (CollectionUtils.isEmpty(existingList)) {
                return Collections.emptyList();
            } else {
                List<PaperDeliveryHighPriority> updatedList = new ArrayList<>(existingList);
                updatedList.removeAll(toRemove);
                return updatedList;
            }
        });
        return Mono.just(toRemove.size());
    }

    @Override
    public List<PaperDeliveryHighPriority> get(String pk) {
        return Optional.ofNullable(data.get(pk))
                .orElse(Collections.emptyList());
    }

    @Override
    public Mono<Page<PaperDeliveryHighPriority>> getChunck(String pk, int limit, Map<String, AttributeValue> lastEvaluatedKey) {
        return Mono.just(Page.create(data.get(pk)
                .stream().limit(limit).toList()));
    }

    public List<PaperDeliveryHighPriority> getAll() {
        return data.values().stream().flatMap(Collection::stream).toList();
    }
}
