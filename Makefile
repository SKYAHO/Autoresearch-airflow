CHART := charts/autoresearch-airflow
RELEASE := autoresearch-airflow
NAMESPACE := airflow
VALUES := environments/gke-values.example.yaml

.PHONY: deps lint template verify

deps:
	helm repo add apache-airflow https://airflow.apache.org || true
	helm repo update
	helm dependency update $(CHART)

lint: deps
	helm lint $(CHART)

template: deps
	helm template $(RELEASE) $(CHART) --namespace $(NAMESPACE) --values $(VALUES) >/tmp/$(RELEASE).yaml

verify: lint template
	git diff --check
