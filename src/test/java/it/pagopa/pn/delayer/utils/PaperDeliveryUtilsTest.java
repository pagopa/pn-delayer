package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PaperDeliveryUtilsTest {

    @Mock
    private PnDelayerConfigs pnDelayerConfig;

    @InjectMocks
    private PaperDeliveryUtils paperDeliveryUtils;


}