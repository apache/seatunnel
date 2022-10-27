# Seatunnel-Spark Setup With Helm


## Install

- First, install the certificate manager;

    ```shell
    kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.7.1/cert-manager.yaml 
    ```

- While you are in the `seatunnel_helm/seatunnel_flink` directory, run the following command:

    ```shell
    helm install seatunnel-flink .
    ```

- To uninstall it:

    ```shell
    helm uninstall seatunnel-flink
    ```

---