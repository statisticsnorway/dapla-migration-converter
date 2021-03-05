.PHONY: default
default: | help

.PHONY: build
build: build-mvn build-docker ## Build all and create docker image

.PHONY: build-mvn
build-mvn: ## Build project and install to you local maven repo
	./mvnw clean install

.PHONY: build-docker
build-docker: ## Build the docker image
	docker build -t rawdata-converter-app-migration:dev -f Dockerfile .

.PHONY: release-dryrun
release-dryrun: ## Simulate a release in order to detect any issues
	./mvnw release:prepare release:perform -Darguments="-Dmaven.deploy.skip=true" -DdryRun=true

.PHONY: release
release: ## Release a new version. Update POMs and tag the new version in git
	./mvnw release:prepare release:perform -Darguments="-Dmaven.deploy.skip=true -Dmaven.javadoc.skip=true"

.PHONY: changelog
changelog: ## Generate CHANGELOG.md
	github_changelog_generator -u statisticsnorway -p rawdata-converter-app-migration

.PHONY: run-local
run-local: ## Run the app locally (without docker)
	MICRONAUT_ENVIRONMENTS=local MICRONAUT_CONFIG_FILES=conf/application-local.yml java -Dcom.sun.management.jmxremote -jar  target/rawdata-converter-app-*.jar

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
