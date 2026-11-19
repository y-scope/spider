# Configuration

:::{warning}
🚧 This section is still under construction.
:::

The guides below describe how to configure each of Spider's components.

Spider's components expose a range of configuration values that let you tune the system's behavior,
such as its expected memory footprint or its scheduling latency. The guides below document the
values that each deployment exposes, along with the default that the deployment ships.

::::{grid} 1 1 2 2
:gutter: 2

:::{grid-item-card}
:link: storage
Storage configuration
^^^
How to configure Spider's storage backend.
:::

:::{grid-item-card}
:link: scheduler
Scheduler configuration
^^^
How to configure Spider's scheduler.
:::

:::{grid-item-card}
:link: worker
Worker configuration
^^^
How to configure Spider's workers.
:::
::::

:::{toctree}
:hidden:

storage.md
scheduler.md
worker.md
:::

The deployments ship a default for every value they expose, so you only need to override the ones
you want to change. Some settings are managed by the deployment itself and can't be overridden;
those are marked `Not supported` in the guides. How you apply an override depends on your deployment
type.

## Kubernetes deployment

The Helm chart's defaults live in [`values.yaml`][helm-values]. There are two ways to override them:

* **Option 1: A custom values file**—Create a file containing only the keys you want to change
  and pass it with `-f`. Helm deep-merges it into the chart's `values.yaml`, so you don't need to
  copy the whole file. `-f` can be repeated, and later files take precedence.

  ```shell
  helm upgrade --install spider tools/deployment/spider-helm -f my-values.yaml
  ```

* **Option 2: Override specific values**—Use Helm's [`--set`][helm-set] option to override
  individual keys when installing or upgrading the chart.

  ```shell
  helm upgrade --install spider tools/deployment/spider-helm \
      --set spiderConfig.storage.runtime.inbound_queue.task_capacity=1048576
  ```

A few caveats:

* List-valued settings, such as `spiderConfig.bundled`, are replaced wholesale rather than merged.
* `--set` infers the type of each value; use `--set-string` for values that must stay strings, such
  as image tags.
* On `helm upgrade`, once you pass any `-f` or `--set`, the values you don't re-supply fall back to
  the chart's defaults. Re-pass the same values file every time, or use `--reuse-values`.
* An upgrade that only changes a config value updates the chart's ConfigMap but doesn't restart the
  pods, so the new config never reaches a running container. Roll the affected components yourself,
  e.g. `kubectl rollout restart deployment/spider-storage`. Run `kubectl get deployments` to find
  the exact names, which depend on the release name.

For each value in the guides, the `Helm value` field names the key to set.

## Docker Compose deployment

Docker Compose renders each component's config file from a template, interpolating environment
variables into it. A few settings, such as the database credentials and the log level, are passed to
the containers as environment variables instead.

[`.env.example`][env-example] lists all supported environment variables and their default values. To
override a default, copy the file to `.env` in the same directory as `compose.yaml`, edit the values
you care about, and recreate the containers:

```shell
cp .env.example .env
docker compose up -d
```

Creating `.env` is optional: every compose file embeds `${VAR:-default}`, so the stack runs on the
defaults if no `.env` exists. Exported shell variables and `--env-file` work as well.

For each value in the guides, the `Docker Compose env var` field names the variable to set.

[env-example]: https://github.com/y-scope/spider/blob/DOCS_VAR_SPIDER_GIT_REF/tools/deployment/spider-compose/.env.example
[helm-set]: https://helm.sh/docs/helm/helm_install/
[helm-values]: https://github.com/y-scope/spider/blob/DOCS_VAR_SPIDER_GIT_REF/tools/deployment/spider-helm/values.yaml
