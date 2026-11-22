package com.shopify.erp;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI erpOpenApi() {
        return new OpenAPI().info(new Info()
                .title("ERP Order-to-Cash API")
                .version("0.1.0")
                .description("Inventory with stock reservation (optimistic locking), purchase orders, and an "
                        + "order saga (reserve stock -> post to ledger -> ship, compensate on failure)."));
    }
}
