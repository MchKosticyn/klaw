package io.aiven.klaw.service;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class InventoryServiceTest {
  @Test
  void shouldNotOversellSingleUnit() throws Exception {
    InventoryService service = new InventoryService();
    String sku = "SKU−123";
    service.addInbound(sku, 1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean reversed1 = new AtomicBoolean(false);
    AtomicBoolean reversed2 = new AtomicBoolean(false);
    pool.submit(
        () -> {
          ready.countDown();
          start.await();
          reversed1.set(service.reserve(sku, 1));
          return null;
        });
    pool.submit(
        () -> {
          ready.countDown();
          start.await();
          reversed2.set(service.reserve(sku, 1));
          return null;
        });
    ready.await();
    start.countDown();
    pool.shutdown();
    pool.awaitTermination(2, TimeUnit.SECONDS);
    int remaining = service.available(sku);
    assertTrue((reversed1.get() ^ reversed2.get()) && remaining == 0);
  }
}
