package io.github.weirdloop;

import io.r2dbc.client.R2dbc;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RawInnerJoinSelectTest {
	private	Logger logger = LoggerFactory.getLogger(RawInnerJoinSelectTest.class);
	
	private static final String SELECT_INNER_JOIN =
			"select * from order_details inner join orders on orders.order_id = "
					+ "order_details.order_id inner join products on products.product_id = "
					+ "order_details.product_id";
	
	@Test
	void test() throws InterruptedException {
		
		CountDownLatch latch = new CountDownLatch(2);
		PostgresqlConnectionConfiguration configuration =
				PostgresqlConnectionConfiguration.builder()
				                                 .host("localhost")
				                                 .database("test-db")
				                                 .username("postgres")
				                                 .password("postgres")
				                                 .build();
		
		R2dbc r2dbc = new R2dbc(new PostgresqlConnectionFactory(configuration));
		r2dbc
				.withHandle(handle ->
						            handle
								            .select(SELECT_INNER_JOIN)
								            .mapResult(result -> result.map((rw, rm) -> rw.get("product_id", Integer.class)))
								            .onErrorResume((Function<Throwable, Publisher<Integer>>) throwable -> {
									            latch.countDown();
									            throw new IllegalArgumentException();
								            })
				           )
				.doOnError(throwable -> {
					logger.error("2. doOnError", throwable);
					latch.countDown();
				})
				.subscribe();
		
		latch.await();
	}
	
}
