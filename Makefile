APP_NAME := $(shell grep '^name = ' Cargo.toml | sed 's/name = "\(.*\)"/\1/')

VERSION_FILE := ./kubernetes/container_version_tag.txt
KUBERNETES_FILE = kubernetes/$(APP_NAME).yaml
IMAGE_NAME = arnecdn/$(APP_NAME)

# Function to get next version from existing images in the repository
define get_next_version
	IMAGES=$$(minikube image ls 2>/dev/null | grep -E '^(docker.io/)?$(IMAGE_NAME):[0-9]+\.[0-9]+$$' || true); \
	if [ -z "$$IMAGES" ]; then \
		echo "0.1"; \
	else \
		VERSIONS=$$(echo "$$IMAGES" | sed -E 's/.*:([0-9]+\.[0-9]+)/\1/' | sort -t. -k1,1n -k2,2n | tail -1); \
		MAJOR=$$(echo "$$VERSIONS" | cut -d. -f1); \
		MINOR=$$(echo "$$VERSIONS" | cut -d. -f2); \
		NEW_MINOR=$$((MINOR + 1)); \
		echo "$$MAJOR.$$NEW_MINOR"; \
	fi
endef

TAG := $(shell $(get_next_version))

.PHONY: build_app all build_image deploy clean clean-images validate-tag get-next-version undeploy

all: deploy clean-images

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

get-next-version:
	@echo "Current images in repository:"; \
	IMAGES=$$(minikube image ls 2>/dev/null | grep -E '^(docker.io/)?$(IMAGE_NAME):[0-9]+\.[0-9]+$$' || true); \
	if [ -z "$$IMAGES" ]; then \
		echo "  (none)"; \
	else \
		echo "$$IMAGES" | sed 's/^/  /'; \
	fi; \
	echo ""; \
	echo "Next version to be built: $(TAG)"

build_image: validate-tag
	@echo "Building Docker image with tag $(TAG)..."
	minikube image build -t "$(IMAGE_NAME):$(TAG)" -f Dockerfile . || { \
		echo "Error: Docker build failed."; \
		exit 1; \
	}

deploy: build_image
	@echo "Applying Kubernetes deployment with TAG=$(TAG)..."
	sed -i '' 's|image: docker.io/$(IMAGE_NAME):.*|image: docker.io/$(IMAGE_NAME):$(TAG)|' $(KUBERNETES_FILE)
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

clean-images:
	@echo "Listing all $(IMAGE_NAME) images in Minikube..."
	@MATCHES=$$(minikube image ls 2>/dev/null | grep -E '^(docker.io/)?$(IMAGE_NAME):[0-9]+\.[0-9]+$$' || true); \
	if [ -z "$$MATCHES" ]; then \
		echo "No matching images found in Minikube."; \
	else \
		echo "Found the following versions:"; \
		echo "$$MATCHES" | sed 's/^/  /'; \
		echo ""; \
		MAX_VERSION=$$(echo "$$MATCHES" | sed -E 's/.*:([0-9]+\.[0-9]+)/\1/' | sort -t. -k1,1n -k2,2n | tail -1); \
		echo "Removing old versions (keeping highest: $$MAX_VERSION)..."; \
		echo "$$MATCHES" | while IFS= read -r IMG; do \
			[ -z "$$IMG" ] && continue; \
			RAW_IMG="$${IMG#docker.io/}"; \
			IMG_TAG="$${RAW_IMG##*:}"; \
			if [ "$$IMG_TAG" != "$$MAX_VERSION" ]; then \
				echo "  Removing: $$RAW_IMG"; \
				minikube image rm "$$RAW_IMG" 2>/dev/null || true; \
				minikube image rm "docker.io/$$RAW_IMG" 2>/dev/null || true; \
			else \
				echo "  Keeping (highest): $$RAW_IMG"; \
			fi; \
		done; \
	fi

clean: clean-images
	@echo "Cleaning local target..."
	cargo clean || true