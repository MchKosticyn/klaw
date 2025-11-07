package io.aiven.klaw.clusterapi.controller;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.aiven.klaw.clusterapi.UtilMethods;
import io.aiven.klaw.clusterapi.models.connect.ConnectorsStatus;
import io.aiven.klaw.clusterapi.models.enums.KafkaSupportedProtocol;
import io.aiven.klaw.clusterapi.services.KafkaConnectService;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(SpringExtension.class)
public class KafkaConnectControllerV2Test {
  private MockMvc mvc;

  @MockBean KafkaConnectService kafkaConnectService;

  UtilMethods utilMethods;

  @BeforeEach
  public void setUp() {
    utilMethods = new UtilMethods();
    KafkaConnectControllerV2 connectControllerV2 = new KafkaConnectControllerV2();
    mvc = MockMvcBuilders.standaloneSetup(connectControllerV2).dispatchOptions(true).build();
    ReflectionTestUtils.setField(
        connectControllerV2, "kafkaConnectService", kafkaConnectService);
  }

  @Test
  public void getAllConnectorsV2Test() throws Exception {
    String getUrl =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.SSL
            + "&cluster=CLID1&includeStatus=true";
    ConnectorsStatus connectors = utilMethods.getConnectorsStatus();
    when(kafkaConnectService.getConnectors(anyString(), any(), anyString(), anyBoolean()))
        .thenReturn(connectors);

    mvc.perform(get(getUrl))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.connectorStateList", hasSize(2)))
        .andExpect(content().string(containsString("conn1")))
        .andExpect(content().string(containsString("conn2")));
  }

  @Test
  public void getAllConnectorsV2WithoutStatusTest() throws Exception {
    String getUrl =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.PLAINTEXT
            + "&cluster=CLID1&includeStatus=false";
    ConnectorsStatus connectors = utilMethods.getConnectorsStatus();
    when(kafkaConnectService.getConnectors(anyString(), any(), anyString(), anyBoolean()))
        .thenReturn(connectors);

    mvc.perform(get(getUrl))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.connectorStateList", hasSize(2)))
        .andExpect(content().string(containsString("conn1")))
        .andExpect(content().string(containsString("conn2")));
  }

  @Test
  public void getAllConnectorsV2InvalidProtocolTest() throws Exception {
    String getUrl =
        "/v2/connectors?host=localhost&protocol=INVALIDPROTOCOL&cluster=CLID1&includeStatus=true";
    ConnectorsStatus connectors = utilMethods.getConnectorsStatus();
    when(kafkaConnectService.getConnectors(anyString(), any(), anyString(), anyBoolean()))
        .thenReturn(connectors);

    mvc.perform(get(getUrl)).andExpect(status().is4xxClientError());
  }

  @Test
  public void getAllConnectorsV2ClusterCallFailureTest() throws Exception {
    String getUrl =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.SSL
            + "&cluster=CLID1&includeStatus=true";
    ConnectorsStatus connectors = new ConnectorsStatus();
    connectors.setConnectorStateList(new ArrayList<>());
    when(kafkaConnectService.getConnectors(anyString(), any(), anyString(), anyBoolean()))
        .thenReturn(connectors);
    mvc.perform(get(getUrl))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.connectorStateList", hasSize(0)));
  }

  @Test
  public void getAllConnectorsV2MissingHostParameterTest() throws Exception {
    String getUrl =
        "/v2/connectors?protocol="
            + KafkaSupportedProtocol.SSL
            + "&cluster=CLID1&includeStatus=true";

    mvc.perform(get(getUrl)).andExpect(status().is4xxClientError());
  }

  @Test
  public void getAllConnectorsV2MissingProtocolParameterTest() throws Exception {
    String getUrl = "/v2/connectors?host=localhost&cluster=CLID1&includeStatus=true";

    mvc.perform(get(getUrl)).andExpect(status().is4xxClientError());
  }

  @Test
  public void getAllConnectorsV2MissingClusterParameterTest() throws Exception {
    String getUrl =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.SSL
            + "&includeStatus=true";

    mvc.perform(get(getUrl)).andExpect(status().is4xxClientError());
  }

  @Test
  public void getAllConnectorsV2MissingIncludeStatusParameterTest() throws Exception {
    String getUrl =
        "/v2/connectors?host=localhost&protocol=" + KafkaSupportedProtocol.SSL + "&cluster=CLID1";

    mvc.perform(get(getUrl)).andExpect(status().is4xxClientError());
  }

  @Test
  public void getAllConnectorsV2AllProtocolsTest() throws Exception {
    // Test PLAINTEXT
    String getUrlPlaintext =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.PLAINTEXT
            + "&cluster=CLID1&includeStatus=true";
    ConnectorsStatus connectors = utilMethods.getConnectorsStatus();
    when(kafkaConnectService.getConnectors(anyString(), any(), anyString(), anyBoolean()))
        .thenReturn(connectors);

    mvc.perform(get(getUrlPlaintext))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.connectorStateList", hasSize(2)));

    // Test SASL_PLAIN
    String getUrlSaslPlain =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.SASL_PLAIN
            + "&cluster=CLID1&includeStatus=true";

    mvc.perform(get(getUrlSaslPlain))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.connectorStateList", hasSize(2)));

    // Test SASL_SSL_PLAIN_MECHANISM
    String getUrlSaslSslPlain =
        "/v2/connectors?host=localhost&protocol="
            + KafkaSupportedProtocol.SASL_SSL_PLAIN_MECHANISM
            + "&cluster=CLID1&includeStatus=true";
      
    mvc.perform(get(getUrlSaslSslPlain))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.connectorStateList", hasSize(2)));
  }
}
