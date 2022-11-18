## Deploying the operator

Install the certificate manager on your Kubernetes cluster to enable adding the webhook component (only needed once per Kubernetes cluster):

```shell
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
```
In case the cert manager installation failed for any reason you can disable the webhook by passing --set webhook.create=false to the helm install command for the operator.

Now you can deploy the selected stable Flink Kubernetes Operator version using the included Helm chart:

```shell
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.2.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```

## Submitting a Flink job

```shell
kubectl create -f seatunnel-flink-v2.yaml
```

You may follow the logs of your job, after a successful startup (which can take on the order of a minute in a fresh environment, seconds afterwards) you can:

```shell
kubectl logs -f deploy/seatunnel-flink-v2-streaming-example
```

To expose the Flink Dashboard you may add a port-forward rule or look the ingress configuration options:

```shell
kubectl port-forward svc/basic-example-rest 8081
```

Now the Flink Dashboard is accessible at localhost:8081.

In order to stop your job and delete your FlinkDeployment you can simply:

```shell
kubectl delete flinkdeployment/seatunnel-flink-v2-streaming-example
```
