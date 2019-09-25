# Jobmon-Kubernetes demo

## Porting to Kuberenetes

I used kompose as a baseline to convert the docker-compose file we're currently
using to Kubernetes configuration files. http://kompose.io/

I also had to add a few extra options to use the minikube-local docker
daemon and to expose services externally.


## Running the demo

1. Make sure you've installed minikube and kubectl as described in:
  - https://kubernetes.io/docs/setup/minikube/
  - https://kubernetes.io/docs/tasks/tools/install-minikube/
  - https://kubernetes.io/docs/tasks/tools/install-kubectl/

2. Start minikube.

  ```
  minikube start
  ```

3. Swap to the minikube-internal docker daemon in your shell session, so
we can build a jobmon image that kubernetes can access.

  ```
  eval $(minikube docker-env)
  ```

4. Prepare jobmon config file and Build the jobmon image using the Dockerfile
   at the repo root.

  ```
  cp jobmonrc-docker jobmonrc-docker-wsecrets
  docker build -t jobmon .
  ```

5. Apply the kubernetes configuration declaratively.

  - https://kubernetes.io/docs/concepts/overview/object-management-kubectl/declarative-config/
  ```
  kubectl apply -f k8s/
  ```

6. Wait a few mins for the db to startup.

  ```
  kubectl get pods
  ```

7. Get the externally exposed ports for each of the jobmon services.

  ```
  minikube service db --url
  minikube service jsm --url
  minikube service jqs --url
  ```

8. When you're finished tinkering, delete the pods.

  ```
  kubectl delete -f k8s/
  ```

9. Stop minikube. If things really went badly and you want to start fresh next
time, you can also delete minikube altogether.

  ```
  minikube stop

  # or

  minikube delete
  ```
