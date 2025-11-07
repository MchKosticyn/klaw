package io.aiven.klaw.clusterapi.controller;

import io.aiven.klaw.clusterapi.models.connect.ConnectorsStatus;
import io.aiven.klaw.clusterapi.models.enums.KafkaSupportedProtocol;
import io.aiven.klaw.clusterapi.services.KafkaConnectService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v2")
@Slf4j
public class KafkaConnectControllerV2 {

  @Autowired KafkaConnectService kafkaConnectService;

  @RequestMapping(
      value = "/connectors",
      method = RequestMethod.GET,
      produces = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ConnectorsStatus> getAllConnectors(
      @RequestParam("host") String kafkaConnectHost,
      @Valid @RequestParam("protocol") KafkaSupportedProtocol protocol,
      @RequestParam("cluster") String clusterIdentification,
      @RequestParam("includeStatus") boolean includeStatus) {
    return new ResponseEntity<>(
        kafkaConnectService.getConnectors(
            kafkaConnectHost, protocol, clusterIdentification, includeStatus),
        HttpStatus.OK);
  }
}
