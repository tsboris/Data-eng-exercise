### Exercise: Data Engineering Pipelines with Apache Airflow

**Objective:**
Set up and execute a data pipeline using Apache Airflow on a Kubernetes cluster, leveraging MinIO for object storage to analyze NBA player heights.


#### Part 1: Install Airflow

Install Apache Airflow on your Kubernetes cluster using the following commands:

```bash
kubectl create namespace airflow
```

If you are using a private git repository, you need to create a secret with your git credentials
```bash
kubectl --namespace airflow create secret generic git-credentials \
  --namespace airflow \
  --from-literal=GITSYNC_USERNAME="" \
  --from-literal=GIT_SYNC_USERNAME="" \
  --from-literal=GITSYNC_PASSWORD="" \
  --from-literal=GIT_SYNC_PASSWORD=""
```

```bash
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-logs
  namespace: airflow
  labels:
    tier: airflow
    component: logs-pvc
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 10Gi
EOF
```
```bash
helm repo add apache-airflow https://airflow.apache.org
```

```bash
cat <<EOF | helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace --values -
executor: "KubernetesExecutor"
statsd:
  enabled: false
useStandardNaming: true
dags:
  gitSync:
    enabled: true
    repo: <git-repo-url>
    branch: main
    rev: HEAD
    depth: 1
    subPath: <sub-path>
    credentialsSecret: git-credentials
    period: 5s
triggerer:
  enabled: true
  persistence:
    enabled: true
    size: 10Gi
    # Execute init container to chown log directory.
    # This is currently only needed in kind, due to usage
    # of local-path provisioner.
    fixPermissions: true
logs:
  persistence:
    enabled: true
    existingClaim: "airflow-logs"
EOF
```

To access the Airflow UI, set up port forwarding:

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```


#### Part 2: Install MinIO

Install MinIO on your Kubernetes cluster:

```bash
helm repo add minio https://charts.min.io/
```

```bash
helm upgrade --install minio minio/minio --namespace minio --create-namespace --set resources.requests.memory=256Mi --set replicas=1 --set mode=standalone --set persistence.size=10Gi --set rootUser=admin,rootPassword=admin123
```

To access the MinIO UI, set up port forwarding:

```bash
kubectl port-forward svc/minio-console 9001:9001 --namespace minio
```

Inside MinIO, create a bucket named `workshop-data`, and access key and secret key for the MinIO S3 connection.
Then, in Airflow, create a connection of type `Amazon Web Services` named `minio_s3` with the access key and secret key you created in MinIO. Use `http://minio.minio.svc.cluster.local:9000` as the endpoint URL in the `Extra` section of the connection. The `Extra` field should look like this:

```json
{
  "endpoint_url": "http://minio.minio.svc.cluster.local:9000"
}
```


#### Part 3: Create a Data Engineering Pipeline for NBA Player Height Analysis

This workshop showcases a comprehensive data engineering pipeline that analyzes NBA player heights. You'll implement a DAG (`nba_height_analysis`) with the following tasks:

1. **Data Extraction**:
   * Download NBA player height data from an external URL: https://www.openintro.org/data/csv/nba_heights.csv
   * The pipeline includes robust error handling with retry logic
   * Upload the raw CSV to MinIO for storage and future reference

> **Note:** Some websites may return HTTP 406 errors when accessed from certain environments. The DAG handles this by including proper request headers and retry logic to improve reliability. The following HTTP headers are explicitly used:
> ```
> headers = {
>     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
>     'Accept': 'text/csv,application/csv,text/plain,application/octet-stream,*/*',
>     'Accept-Language': 'en-US,en;q=0.9',
>     'Referer': 'https://www.openintro.org/'
> }
> ```

2. **Data Cleaning**:
   * Create a full player name column by combining first and last names
   * Display heights in metric units (meters and centimeters) as primary measurements
   * Categorize players by position based on their height in meters
   * Sort players by height and reset index for improved organization
   * Store insightful statistics about the tallest and shortest players in logs
   * Upload the cleaned data to MinIO

3. **Data Analysis and Visualization**:
   * Calculate statistics by position category using metric measurements
   * Generate ASCII-based histograms showing the distribution of player heights in centimeters
   * Create text-based box plots visualizing height differences across position categories
   * Identify the top 5 tallest and shortest players with their metric measurements
   * Generate a comprehensive JSON report with all analysis results
   * Upload the analysis results to MinIO


4. **Dashboard Generation**:
   * Create an HTML dashboard with ASCII/text-based visualizations
   * Format the data in readable tables
   * Apply modern styling with responsive design
   * Upload the dashboard HTML to MinIO for easy viewing

The pipeline demonstrates key data engineering concepts including data extraction, cleaning, transformation, visualization, and storage in a real-world context that workshop participants can relate to.


#### Part 4: Validation and Monitoring

* Check Airflow UI for DAG run status, task logs, and execution times
* View the task logs to see interesting statistics about NBA players
* Download and view the generated dashboard HTML from MinIO
* Explore the analysis results in the JSON file
