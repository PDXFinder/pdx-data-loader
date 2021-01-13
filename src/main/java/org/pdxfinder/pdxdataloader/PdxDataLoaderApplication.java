package org.pdxfinder.pdxdataloader;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class PdxDataLoaderApplication {

	public static void main(String[] args) {
		SpringApplication.run(PdxDataLoaderApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			PatientDataLoader patientDataLoader = new PatientDataLoader();
			patientDataLoader.load();

		};
	}
}
