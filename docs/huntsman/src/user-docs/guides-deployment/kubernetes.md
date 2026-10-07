# Kubernetes deployment

This guide describes how to deploy a Spider cluster on Kubernetes using the [Spider Helm
chart][helm-chart].

---

## Requirements

* [`kubectl`][kubectl] >= 1.30
* [Helm] >= 4.0
* A Kubernetes cluster (see [Setting up a cluster](#setting-up-a-cluster) below)

---

## Setting up a cluster

If you already have a cluster, skip to [Installing the chart](#installing-the-chart). Otherwise,
[`kind`][kind] (Kubernetes in Docker) runs a cluster inside Docker containers, making it ideal for
local testing and development.

`kind` requires:

* [Docker], which requires:
  * `containerd.io` >= 1.7.18
  * `docker-ce` >= 27.0.3
  * `docker-ce-cli` >= 27.0.3
* [`kind`][kind] >= 0.23

Create a `kind` cluster:

```shell
kind create cluster --name spider
```

---

## Installing the chart

### Adding the Helm repository

The chart is published to a Helm repository hosted on the `gh-pages` branch of Spider's GitHub
repository:

```shell
helm repo add spider https://github.com/y-scope/spider/raw/gh-pages
helm repo update spider
```

### Basic installation

To install the chart with its default values:

```shell
helm install spider spider/spider
```

### Installation with custom values

For highly customized deployments, you can override the default values by creating a values file.
The chart's defaults live in [`values.yaml`][helm-values], and Helm deep-merges your file into
them, so you only need to list the keys you want to change:

```{code-block} yaml
:caption: spider-values.yaml

# Use custom image tags.
image:
  scheduler:
    tag: "latest"
  storage:
    tag: "latest"
  worker:
    tag: "latest"

spiderConfig:
  # Tune performance and resource usage.
  scheduler:
    runtime:
      scheduler:
        config:
          active_job_queue_capacity: 64
          dispatch_queue_capacity: 64

  # Adjust worker horizontal scaling.
  worker:
    replicas: 8
```

Install the chart with the custom values file:

```shell
helm install spider spider/spider -f spider-values.yaml
```

:::{note}
The example above shows only a subset of the available settings. For the complete set of settings
that tune each component's runtime behavior, see the [Configuration][configuration] guides. For more
advanced deployment settings, see [Advanced deployment settings](#advanced-deployment-settings)
below.
:::

---

## Verifying the deployment

After installing the Helm chart, you can verify that all components are running correctly as
follows.

### Check pod status

Wait for all pods to be ready:

```shell
# Watch pod status
kubectl get pods -w

# Wait for all pods to be ready
kubectl wait pods --all --for=condition=Ready --timeout=300s
```

The output should show that all pods are in the `Running` state:

```text
NAME                  READY   STATUS    RESTARTS   AGE
spider-database-0     1/1     Running   0          2m
spider-scheduler-...  1/1     Running   2          2m
spider-storage-...    1/1     Running   2          2m
spider-worker-...     1/1     Running   0          2m
```

:::{note}
Spider's services fail fast when their dependencies are unreachable, so the storage and scheduler
pods may restart a few times while the database is initializing. A small number of restarts during
startup is expected.
:::

---

## Advanced deployment settings

### Scaling the workers

`spiderConfig.worker.replicas` (default: `4`): Sets the number of worker pods. Increase it to
raise the number of tasks the cluster can execute concurrently.

### Making task packages available to the workers

The default worker image ships with the execution manager and task executor, but no pre-installed
TDL packages. You can supply your TDL packages in one of two ways:

#### Option 1: Mount a volume

Mount a volume containing your built packages. Use `extra_volumes` to specify the package source
and `extra_volume_mounts` to define where the worker reads them.

:::{note}
The `mountPath` must match `spiderConfig.execution_manager.task_executor.package_dir` in your
values file (or its default: `/opt/spider/packages`).
:::

```{code-block} yaml
:caption: spider-values.yaml

spiderConfig:
  worker:
    extra_volumes:
      - name: "task-packages"
        hostPath:
          path: "/path/to/your/packages"
          type: "Directory"
    extra_volume_mounts:
      - name: "task-packages"
        mountPath: "/opt/spider/packages"  # Default package_dir.
        readOnly: true
```

#### Option 2: Build a custom worker image

For TDL packages requiring complex dependencies or runtimes, build a custom container image using
`ghcr.io/y-scope/spider/worker` as the base:

1. Create a `Dockerfile` starting `FROM ghcr.io/y-scope/spider/worker` that installs your TDL
   package and required dependencies.

2. Override `image.worker.repository` and `image.worker.tag` in your Helm values file to point to
   your container image.

### Passing environment variables to tasks

Once a TDL package is available, you may pass environment variables through Spider's Helm chart
values file so tasks can consume them at runtime. Forwarding variables requires configuring two
settings:

* `spiderConfig.worker.extra_envs`: Adds environment variables to the execution manager container.

* `spiderConfig.execution_manager.task_executor.inherited_env`: Lists the variables to forward from
  the execution manager to the task executors.

:::{important}
A variable must be listed in both fields to be accessible by a running task.
:::

#### Example configuration

To pass the `AWS_REGION` environment variable to your tasks:

```{code-block} yaml
:caption: spider-values.yaml

spiderConfig:
  execution_manager:
    task_executor:
      inherited_env: ["AWS_REGION"]

  worker:
    extra_envs:
      - name: "AWS_REGION"
        value: "us-east-2"
```

[configuration]: ../guides-configuration/index.md
[Docker]: https://docs.docker.com/engine/install/
[Helm]: https://helm.sh/
[helm-chart]: https://github.com/y-scope/spider/tree/main/tools/deployment/spider-helm
[helm-values]: https://github.com/y-scope/spider/blob/main/tools/deployment/spider-helm/values.yaml
[kind]: https://kind.sigs.k8s.io/
[kubectl]: https://kubernetes.io/docs/tasks/tools/
