# K8s IT — operator-facing lifecycle commands.
#
# THREE tofu roots live here, all operator-owned and all SHARED by every tier-3 suite (which
# is why they sit at the repo root rather than under an example — a suite's example may come
# and go, the cluster does not):
#
#   cluster/          ← kind cluster + local registry. Provider-light; one clean apply.
#                       Slow (~3-5 min); operator runs once per local environment.
#   infra/            ← MetalLB, ingress-nginx, KEDA, EFK + the OTel observability
#                       pipeline. Applied against the already-running cluster via its
#                       kubeconfig FILE. Slow (~5-8 min); operator runs once.
#   shared-scenario/  ← the R14 shared engine instance (engine + postgres + rabbitmq +
#                       estate Secret + deployments ConfigMap + Ingress). The per-example
#                       scenario stage RETIRED with R14: packages hot-deploy onto this ONE
#                       engine via `sutra deploy`, and the suites re-apply it idempotently.
#
# Per user direction 2026-05-24: "IT apply and destroy must be local to the IT
# scenario. For the local environment there should be a separate init and destroy
# steps. ... For the IT tests, don't re-create and destroy the local cluster."
#
# Operator workflow:
#
#   make init       # bring the cluster + infra up (one-time)
#   make ready      # show cluster status (kubectl get nodes / pods)
#   make shared-apply     # provision the shared engine instance (R14) — or let the ITs do it
#   (cd rust && cargo test -p sutra-conformance -- --ignored --test-threads=1 k8s_)  # hot-deploy suites
#   make destroy    # tear down infra + cluster
#
# Prereqs for the shared instance: the Rust engine image pushed to the kind registry
# (docker build -t localhost:5000/sutra-engine:k8s-it -f rust/Dockerfile rust/ && docker push
# localhost:5000/sutra-engine:k8s-it) and the sutra CLI built (cd rust && cargo build -p
# sutra-cli --release).

CLUSTER_DIR := cluster
INFRA_DIR := infra
# The R14 shared-instance harness — a plain sibling since the cluster/infra stages moved here.
SHARED_SCENARIO_DIR := shared-scenario

# kubeconfig the cluster stage wrote — infra/ + the shared scenario providers read it, and so
# does the conformance harness (whose default is cluster/sutra-fednow-it-config).
# Read with an ABSOLUTE -chdir (not `cd $(CLUSTER_DIR) && ...`): the substitution
# is expanded inside a recipe line that has already `cd`-ed into infra/, so a
# relative `cd cluster` would resolve against the wrong dir and return an empty
# path. $(CURDIR) is the Makefile's own directory.
KUBECONFIG_PATH = $$(tofu -chdir="$(CURDIR)/$(CLUSTER_DIR)" output -raw kubeconfig_path)

.PHONY: init cluster-apply infra-apply ready destroy shared-apply shared-destroy clean help

init: cluster-apply infra-apply ## Bring up the kind cluster + infra (one-time per local environment).

cluster-apply: ## Stage 1: kind cluster + registry. One clean apply (no targeted hack — provider-light root).
	cd $(CLUSTER_DIR) && tofu init -input=false
	cd $(CLUSTER_DIR) && tofu apply -auto-approve -input=false

infra-apply: ## Stage 2: MetalLB + ingress + KEDA + EFK + observability, against the running cluster.
	cd $(INFRA_DIR) && tofu init -input=false
	cd $(INFRA_DIR) && tofu apply -auto-approve -input=false \
		-var "kubeconfig_path=$(KUBECONFIG_PATH)"

ready: ## Show cluster readiness — node count, prod-dep pod status.
	@kubectl get nodes -o wide
	@echo "---"
	@kubectl -n metallb-system get pods 2>/dev/null || echo "(metallb not installed)"
	@kubectl -n ingress-nginx get pods 2>/dev/null || echo "(ingress-nginx not installed)"
	@kubectl -n keda get pods 2>/dev/null || echo "(keda not installed)"
	@kubectl -n elastic-system get pods 2>/dev/null || echo "(eck-operator not installed)"
	@kubectl get elasticsearch 2>/dev/null || echo "(elasticsearch CR not provisioned)"
	@kubectl get kibana 2>/dev/null || echo "(kibana CR not provisioned)"

destroy: ## Tear down infra THEN the kind cluster (operator-initiated only; ITs never call this).
	cd $(INFRA_DIR) && tofu destroy -auto-approve -input=false \
		-var "kubeconfig_path=$(KUBECONFIG_PATH)" || true
	cd $(CLUSTER_DIR) && tofu destroy -auto-approve -input=false

shared-apply: ## Stage 3 (R14): the shared engine instance — engine + postgres + rabbitmq + deployments ConfigMap + estate Secret + Ingress. ITs re-apply idempotently.
	cd $(SHARED_SCENARIO_DIR) && tofu init -input=false && tofu apply -auto-approve -input=false \
		-var "kubeconfig_path=$(KUBECONFIG_PATH)"

shared-destroy: ## Tear down just the shared engine instance (undeploy packages first: sutra undeploy <archive>).
	cd $(SHARED_SCENARIO_DIR) && tofu destroy -auto-approve -input=false \
		-var "kubeconfig_path=$(KUBECONFIG_PATH)"

clean: ## Wipe local tofu state + provider plugins under the operator-owned subdirs (BE CAREFUL — only after destroy).
	rm -rf $(CLUSTER_DIR)/.terraform $(CLUSTER_DIR)/.terraform.lock.hcl $(CLUSTER_DIR)/terraform.tfstate*
	rm -rf $(INFRA_DIR)/.terraform $(INFRA_DIR)/.terraform.lock.hcl $(INFRA_DIR)/terraform.tfstate*

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
