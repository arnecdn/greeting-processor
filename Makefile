APP_NAME = greeting-processor

IMAGE_NAME = arnecdn/$(APP_NAME)
KUBERNETES_FILE = kubernetes/$(APP_NAME).yaml
VERSION_FILE := ./kubernetes/container_version_tag.txt
TAG := $(shell [ -f $(VERSION_FILE) ] || echo "0.1" > $(VERSION_FILE); cat $(VERSION_FILE))

.PHONY: build_app all build_image deploy clean validate-tag increment-version undeploy

all: build_image deploy

build_app:
	@echo "Building the application..."
	cargo build --release || { \
		echo "Error: Cargo build failed."; \
		exit 1; \
	}

validate-tag:
	@if ! echo $(TAG) | grep -Eq '^[0-9]+\.[0-9]+$$'; then \
		echo "Error: Invalid tag format. Must be in the form of 'X.Y' where X and Y are integers."; \
		exit 1; \
	fi

increment-version:
	@MAJOR=$$(echo $(TAG) | cut -d. -f1); \
	MINOR=$$(echo $(TAG) | cut -d. -f2); \
	NEW_MINOR=$$((MINOR + 1)); \
	NEW_TAG="$$MAJOR.$$NEW_MINOR"; \
	echo "$$NEW_TAG" > $(VERSION_FILE); \
	echo "Version incremented: $(TAG) -> $$NEW_TAG"

build_image: validate-tag increment-version
	$(eval TAG := $(shell cat $(VERSION_FILE)))
	@echo "Building Docker image with tag $(TAG)..."
	minikube image build -t "$(IMAGE_NAME):$(TAG)" -f Dockerfile . || { \
		echo "Error: Docker build failed."; \
		exit 1; \
	}

deploy: build_image
	@echo "Applying Kubernetes deployment with TAG=$(TAG)..."
	sed -i '' 's|image: docker.io/arnecdn/greeting-receiver:.*|image: docker.io/arnecdn/greeting-receiver:$(TAG)|' $(KUBERNETES_FILE)
	kubectl apply -f $(KUBERNETES_FILE) || { \
		echo "Error: Failed to apply Kubernetes deployment."; \
		exit 1; \
	}

undeploy:
	@echo "Removing Kubernetes deployment..."
	kubectl delete -f $(KUBERNETES_FILE) || { \
		echo "Error: Failed to delete Kubernetes deployment."; \
		exit 1; \
	}

clean:
	@echo "Cleaning up..."
	@echo "Removing image from Minikube..."
	minikube image rm "$(IMAGE_NAME):$(TAG)" 2>/dev/null || true
	@echo "Cleaning local target..."
	cargo clean || true